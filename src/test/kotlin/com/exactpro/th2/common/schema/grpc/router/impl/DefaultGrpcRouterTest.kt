/*
 *  Copyright 2023 Exactpro (Exactpro Systems Limited)
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.exactpro.th2.common.schema.grpc.router.impl

import com.exactpro.th2.common.annotations.IntegrationTest
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcEndpointConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRawRobinStrategy
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRetryConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServerConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServiceConfiguration
import com.exactpro.th2.common.schema.strategy.route.impl.RobinRoutingStrategy
import com.exactpro.th2.common.test.grpc.AsyncTestService
import com.exactpro.th2.common.test.grpc.Request
import com.exactpro.th2.common.test.grpc.Response
import com.exactpro.th2.common.test.grpc.TestGrpc.TestImplBase
import com.exactpro.th2.common.test.grpc.TestService
import com.exactpro.th2.service.AbstractGrpcService.STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST
import io.grpc.Context
import io.grpc.Deadline
import io.grpc.Server
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.timeout
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.testcontainers.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

private const val CANCEL_REASON = "test request is canceled"

@IntegrationTest
internal class DefaultGrpcRouterTest {
    @IntegrationTest
    abstract inner class AbstractGrpcRouterTest {
        private val grpcRouterClient = DefaultGrpcRouter()
        private val grpcRouterServer = DefaultGrpcRouter()
        protected val executor: ExecutorService = Executors.newSingleThreadExecutor(
            ThreadFactoryBuilder().setNameFormat("test-%d").build()
        )
        protected val deadlineExecutor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
            ThreadFactoryBuilder().setNameFormat("test-deadline-%d").build()
        )

        @AfterEach
        fun afterEach() {
            grpcRouterServer.close()
            grpcRouterClient.close()
            executor.shutdownGracefully()
            deadlineExecutor.shutdownGracefully()
        }

        abstract fun general()
        abstract fun `delayed server start`()
        abstract fun `cancel retry request`()
        abstract fun `deadline retry request`()
        abstract fun `interrupt thread during retry request`()
        abstract fun `server terminated intermediate session (retry false)`()
        abstract fun `server terminated intermediate session (retry true)`()

        protected fun createClientSync(
            configuration: GrpcServiceConfiguration = GrpcServiceConfiguration(
                RobinRoutingStrategy().apply { init(GrpcRawRobinStrategy(listOf("endpoint"))) },
                TestService::class.java,
                mapOf("endpoint" to GrpcEndpointConfiguration("localhost", SERVER_PORT))
            ),
            retryInterruptedTransaction: Boolean = false
        ): TestService {
            grpcRouterClient.init(
                GrpcConfiguration(services = mapOf("test" to configuration)),
                GrpcRouterConfiguration(
                    retryConfiguration = GrpcRetryConfiguration(
                        Int.MAX_VALUE,
                        RETRY_TIMEOUT,
                        RETRY_TIMEOUT,
                        retryInterruptedTransaction
                    )
                )
            )
            return grpcRouterClient.getService(TestService::class.java)
        }

        protected fun createClientAsync(
            configuration: GrpcServiceConfiguration = GrpcServiceConfiguration(
                RobinRoutingStrategy().apply { init(GrpcRawRobinStrategy(listOf("endpoint"))) },
                TestService::class.java,
                mapOf("endpoint" to GrpcEndpointConfiguration("localhost", SERVER_PORT))
            ),
            retryInterruptedTransaction: Boolean = false
        ): AsyncTestService {
            grpcRouterClient.init(
                GrpcConfiguration(services = mapOf("test" to configuration)),
                GrpcRouterConfiguration(
                    retryConfiguration = GrpcRetryConfiguration(
                        Int.MAX_VALUE,
                        RETRY_TIMEOUT,
                        RETRY_TIMEOUT,
                        retryInterruptedTransaction
                    )
                )
            )
            return grpcRouterClient.getService(AsyncTestService::class.java)
        }

        protected fun createServer(
            configuration: GrpcServerConfiguration = GrpcServerConfiguration(
                null,
                SERVER_PORT
            ),
            grpcRouter: DefaultGrpcRouter = grpcRouterServer,
            completeResponse: Boolean = true,
            handlerBaton: Baton? = null,
        ): Server {
            grpcRouter.init(GrpcConfiguration(serverConfiguration = configuration), GrpcRouterConfiguration())
            return grpcRouter.startServer(TestServiceHandler(completeResponse, handlerBaton)).apply(Server::start)
        }
    }

    @Nested
    inner class SingleRequestSingleResponseSyncTest : AbstractGrpcRouterTest() {
        @Test
        override fun general() {
            createServer().execAndClose(true) {
                val response = executor.submit<Response> {
                    return@submit createClientSync().singleRequestSingleResponse(createRequest())
                }.get(1, TimeUnit.MINUTES)

                assertEquals(1, response.seq)
                assertEquals(1, response.origSeq)
            }
        }

        @Test
        override fun `delayed server start`() {
            val clientServerBaton = Baton("client-server")
            val future = executor.submit<Response> {
                clientServerBaton.give("client thread started")
                return@submit createClientSync().singleRequestSingleResponse(createRequest())
            }

            clientServerBaton.get("wait client thread start")
            Thread.sleep(RETRY_TIMEOUT / 2)

            createServer().execAndClose {
                val response = future.get(1, TimeUnit.MINUTES)
                assertEquals(1, response.seq)
                assertEquals(1, response.origSeq)
            }
        }

        @Test
        override fun `cancel retry request`() {
            val clientServerBaton = Baton("client-server")
            val grpcContext = AtomicReference<Context.CancellableContext>()

            val future = executor.submit<Response> {
                grpcContext.set(
                    Context.current()
                        .withCancellation()
                )

                clientServerBaton.give("client thread started")
                grpcContext.get().call {
                    createClientSync().singleRequestSingleResponse(createRequest())
                }
            }

            clientServerBaton.get("wait client thread start")
            Thread.sleep(RETRY_TIMEOUT / 2)
            assertTrue(grpcContext.get().cancel(RuntimeException(CANCEL_REASON)))

            val exception = assertThrows<ExecutionException> {
                future.get(1, TimeUnit.MINUTES)
            }
            assertCanceledSync(exception)
        }

        @Test
        override fun `deadline retry request`() {
            val clientServerBaton = Baton("client-server")

            val future = executor.submit<Response> {
                clientServerBaton.give("client thread started")
                Context.current()
                    .withDeadline(Deadline.after(RETRY_TIMEOUT / 2, TimeUnit.MILLISECONDS), deadlineExecutor)
                    .call {
                        createClientSync().singleRequestSingleResponse(createRequest())
                    }
            }

            clientServerBaton.get("wait client thread start")

            val exception = assertThrows<ExecutionException> {
                future.get(1, TimeUnit.MINUTES)
            }
            assertDeadlineExceededSync(exception)
        }

        @Test
        override fun `interrupt thread during retry request`() {
            val clientServerBaton = Baton("client-server")

            val future = executor.submit<Response> {
                clientServerBaton.give("client thread started")
                createClientSync().singleRequestSingleResponse(createRequest())
            }

            clientServerBaton.get("wait client thread start")
            Thread.sleep(RETRY_TIMEOUT / 2)

            assertEquals(0, executor.shutdownNow().size)

            val exception = assertThrows<ExecutionException> {
                future.get(1, TimeUnit.MINUTES)
            }
            assertInterruptedSync(exception)
        }

        @Test
        override fun `server terminated intermediate session (retry false)`() {
            val clientServerBaton = Baton("client-server")
            val future = executor.submit<Response> {
                clientServerBaton.giveAndGet("client thread started", "wait server start")
                createClientSync().singleRequestSingleResponse(createRequest())
            }

            val handlerBaton = Baton("handler")

            createServer(completeResponse = false, handlerBaton = handlerBaton).execAndClose(true) {
                clientServerBaton.get("wait client thread start")
                clientServerBaton.give("server started")
                handlerBaton.get("wait response sent")
                Thread.sleep(RETRY_TIMEOUT / 2)
            }

            val exception = assertThrows<ExecutionException> {
                future.get(1, TimeUnit.MINUTES)
            }
            K_LOGGER.error(exception) { "Handle exception" }

            assertException(
                exception, ExceptionMetadata(
                    "java.lang.RuntimeException: Can not execute GRPC blocking request",
                    ExceptionMetadata(
                        "Can not execute GRPC blocking request",
                        suspended = listOf(
                            ExceptionMetadata(
                                "CANCELLED: $STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST",
                            ),
                        )
                    ),
                )
            )
        }

        @Test
        override fun `server terminated intermediate session (retry true)`() {
            val clientServerBaton = Baton("client-server")
            val future = executor.submit<Response> {
                clientServerBaton.giveAndGet("client thread started", "wait server start")
                createClientSync(retryInterruptedTransaction = true).singleRequestSingleResponse(
                    createRequest()
                )
            }

            val handlerBaton = Baton("handler")

            createServer(completeResponse = false, handlerBaton = handlerBaton).execAndClose(true) {
                clientServerBaton.get("wait client thread start")
                clientServerBaton.give("server started")
                handlerBaton.get("wait response sent")
                Thread.sleep(RETRY_TIMEOUT / 2)
            }

            assertFalse(future.isDone)

            DefaultGrpcRouter().use { grpcRouter ->
                createServer(grpcRouter = grpcRouter).execAndClose {
                    val response = future.get(1, TimeUnit.MINUTES)
                    assertEquals(1, response.seq)
                    assertEquals(1, response.origSeq)
                }
            }
        }
    }

    @Nested
    inner class SingleRequestSingleResponseAsyncTest : AbstractGrpcRouterTest() {
        @Test
        override fun general() {
            val streamObserver = mock<StreamObserver<Response>> { }
            createServer().execAndClose(true) {
                createClientAsync().singleRequestSingleResponse(createRequest(), streamObserver)

                val captor = argumentCaptor<Response> { }
                verify(streamObserver, timeout(60 * 1_000)).onCompleted()
                verify(streamObserver).onNext(captor.capture())
                verifyNoMoreInteractions(streamObserver)

                assertEquals(1, captor.allValues.size)
                assertEquals(1, captor.firstValue.seq)
                assertEquals(1, captor.firstValue.origSeq)
            }
        }

        @Test
        override fun `delayed server start`() {
            val streamObserver = mock<StreamObserver<Response>> { }
            createClientAsync().singleRequestSingleResponse(createRequest(), streamObserver)

            Thread.sleep(RETRY_TIMEOUT / 2)

            createServer().execAndClose {
                val captor = argumentCaptor<Response> { }
                verify(streamObserver, timeout(60 * 1_000)).onCompleted()
                verify(streamObserver).onNext(captor.capture())
                verifyNoMoreInteractions(streamObserver)

                assertEquals(1, captor.allValues.size)
                assertEquals(1, captor.firstValue.seq)
                assertEquals(1, captor.firstValue.origSeq)
            }
        }

        @Test
        override fun `cancel retry request`() {
            val grpcContext = Context.current().withCancellation()

            val streamObserver = mock<StreamObserver<Response>> { }
            grpcContext.call {
                createClientAsync().singleRequestSingleResponse(createRequest(), streamObserver)
            }

            Thread.sleep(RETRY_TIMEOUT / 2)

            assertTrue(grpcContext.cancel(RuntimeException(CANCEL_REASON)))

            val captor = argumentCaptor<Throwable> { }
            verify(streamObserver, timeout(60 * 1_000)).onError(captor.capture())
            verifyNoMoreInteractions(streamObserver)

            assertEquals(1, captor.allValues.size)
            K_LOGGER.error(captor.firstValue) { "Handle exception" }

            assertException(
                captor.firstValue, ExceptionMetadata(
                    "CANCELLED: Context cancelled",
                    ExceptionMetadata(
                        CANCEL_REASON,
                    )
                )
            )
        }

        @Test
        override fun `deadline retry request`() {
            val grpcContext = Context.current()
                .withDeadline(Deadline.after(RETRY_TIMEOUT / 2, TimeUnit.MILLISECONDS), deadlineExecutor)

            val streamObserver = mock<StreamObserver<Response>> { }
            grpcContext.call {
                createClientAsync().singleRequestSingleResponse(createRequest(), streamObserver)
            }

            val captor = argumentCaptor<Throwable> { }
            verify(streamObserver, timeout(60 * 1_000)).onError(captor.capture())
            verifyNoMoreInteractions(streamObserver)

            assertEquals(1, captor.allValues.size)
            K_LOGGER.error(captor.firstValue) { "Handle exception" }

            assertException(
                captor.firstValue, ExceptionMetadata(
                    "DEADLINE_EXCEEDED: context timed out",
                    ExceptionMetadata(
                        "context timed out",
                    )
                )
            )
        }

        @Test
        override fun `interrupt thread during retry request`() {
            // this test isn't relevant for async request
        }

        @Test
        override fun `server terminated intermediate session (retry false)`() {
            val streamObserver = mock<StreamObserver<Response>> { }
            val handlerBaton = Baton("handler")

            createServer(completeResponse = false, handlerBaton = handlerBaton).execAndClose(true) {
                createClientAsync().singleRequestSingleResponse(createRequest(), streamObserver)

                handlerBaton.get("wait response sent")
                Thread.sleep(RETRY_TIMEOUT / 2)
            }

            val captor = argumentCaptor<Throwable> { }
            verify(streamObserver, timeout(60 * 1_000)).onError(captor.capture())
            verifyNoMoreInteractions(streamObserver)

            assertEquals(1, captor.allValues.size)
            K_LOGGER.error(captor.firstValue) { "Handle exception" }

            assertException(
                captor.firstValue, ExceptionMetadata(
                    "CANCELLED: $STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST",
                )
            )
        }

        @Test
        override fun `server terminated intermediate session (retry true)`() {
            val streamObserver = mock<StreamObserver<Response>> { }
            val handlerBaton = Baton("handler")

            createServer(completeResponse = false, handlerBaton = handlerBaton).execAndClose(true) {
                createClientAsync(retryInterruptedTransaction = true).singleRequestSingleResponse(
                    createRequest(),
                    streamObserver
                )
                handlerBaton.get("wait response sent")
                Thread.sleep(RETRY_TIMEOUT / 2)
            }

            verifyNoMoreInteractions(streamObserver)

            DefaultGrpcRouter().use { grpcRouter ->
                createServer(grpcRouter = grpcRouter).execAndClose {
                    val captor = argumentCaptor<Response> { }
                    verify(streamObserver, timeout(60 * 1_000)).onCompleted()
                    verify(streamObserver).onNext(captor.capture())
                    verifyNoMoreInteractions(streamObserver)

                    assertEquals(1, captor.allValues.size)
                    assertEquals(1, captor.firstValue.seq)
                    assertEquals(1, captor.firstValue.origSeq)
                }
            }
        }
    }

    @Nested
    inner class SingleRequestMultipleResponseSyncTest : AbstractGrpcRouterTest() {
        @Test
        override fun general() {
            createServer().execAndClose {
                val responses = createClientSync().singleRequestMultipleResponse(createRequest())
                    .asSequence().toList()

                assertEquals(2, responses.size)
                responses.forEachIndexed { index, response ->
                    assertEquals(index + 1, response.seq)
                    assertEquals(1, response.origSeq)
                }
            }
        }

        @Test
        override fun `delayed server start`() {
            val clientServerBaton = Baton("client-server")
            val future = executor.submit<List<Response>> {
                clientServerBaton.give("client thread started")
                return@submit createClientSync().singleRequestMultipleResponse(createRequest())
                    .asSequence().toList()
            }

            clientServerBaton.get("wait client thread start")
            Thread.sleep(RETRY_TIMEOUT / 2)

            createServer().execAndClose {
                val response = future.get(1, TimeUnit.MINUTES)
                assertEquals(2, response.size)
            }
        }

        @Test
        override fun `cancel retry request`() {
            val clientServerBaton = Baton("client-server")
            val grpcContext = AtomicReference<Context.CancellableContext>()

            val future = executor.submit<List<Response>> {
                grpcContext.set(
                    Context.current()
                        .withCancellation()
                )

                clientServerBaton.give("client thread started")
                grpcContext.get().call {
                    createClientSync().singleRequestMultipleResponse(createRequest())
                        .asSequence().toList()
                }
            }

            clientServerBaton.get("wait client thread start")
            Thread.sleep(RETRY_TIMEOUT / 2)
            assertTrue(grpcContext.get().cancel(RuntimeException(CANCEL_REASON)))

            val exception = assertThrows<ExecutionException> {
                future.get(1, TimeUnit.MINUTES)
            }
            assertCanceledSync(exception)
        }

        @Test
        override fun `deadline retry request`() {
            val clientServerBaton = Baton("client-server")

            val future = executor.submit<List<Response>> {
                clientServerBaton.give("client thread started")
                Context.current()
                    .withDeadline(Deadline.after(RETRY_TIMEOUT / 2, TimeUnit.MILLISECONDS), deadlineExecutor)
                    .call {
                        createClientSync().singleRequestMultipleResponse(createRequest()).asSequence().toList()
                    }
            }

            clientServerBaton.get("wait client thread start")

            val exception = assertThrows<ExecutionException> {
                future.get(1, TimeUnit.MINUTES)
            }
            assertDeadlineExceededSync(exception)
        }

        @Test
        override fun `interrupt thread during retry request`() {
            val clientServerBaton = Baton("client-server")

            val future = executor.submit<List<Response>> {
                clientServerBaton.give("client thread started")
                createClientSync().singleRequestMultipleResponse(createRequest()).asSequence().toList()
            }

            clientServerBaton.get("wait client thread start")
            Thread.sleep(RETRY_TIMEOUT / 2)

            assertEquals(0, executor.shutdownNow().size)

            val exception = assertThrows<ExecutionException> {
                future.get(1, TimeUnit.MINUTES)
            }
            assertInterruptedSync(exception)
        }

        @Test
        override fun `server terminated intermediate session (retry false)`() {
            val clientServerBaton = Baton("client-server")
            val future = executor.submit<List<Response>> {
                clientServerBaton.giveAndGet("client thread started", "wait server start")
                createClientSync().singleRequestMultipleResponse(createRequest())
                    .asSequence().toList()
            }

            val handlerBaton = Baton("handler")

            createServer(completeResponse = true, handlerBaton = handlerBaton).execAndClose(true) {
                clientServerBaton.get("wait client thread start")
                clientServerBaton.give("server started")
                handlerBaton.get("wait response sent")
                Thread.sleep(RETRY_TIMEOUT / 2)
            }

            val exception = assertThrows<ExecutionException> {
                future.get(1, TimeUnit.MINUTES)
            }
            K_LOGGER.error(exception) { "Handle exception" }
            assertException(
                exception, ExceptionMetadata(
                    "java.lang.IllegalStateException: Request failures mid-transfer",
                    ExceptionMetadata(
                        "Request failures mid-transfer",
                        ExceptionMetadata(
                            "CANCELLED: $STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST"
                        )
                    )
                )
            )
        }

        @Test
        override fun `server terminated intermediate session (retry true)`() {
            val clientServerBaton = Baton("client-server")
            val future = executor.submit<List<Response>> {
                clientServerBaton.giveAndGet("client thread started", "wait server start")
                createClientSync(retryInterruptedTransaction = true).singleRequestMultipleResponse(
                    createRequest()
                ).asSequence().toList()
            }

            val handlerBaton = Baton("handler")

            createServer(completeResponse = true, handlerBaton = handlerBaton).execAndClose(true) {
                clientServerBaton.get("wait client thread start")
                clientServerBaton.give("server started")
                handlerBaton.get("wait response sent")
                Thread.sleep(RETRY_TIMEOUT / 2)
            }

            val exception = assertThrows<ExecutionException> {
                future.get(1, TimeUnit.MINUTES)
            }
            K_LOGGER.error(exception) { "Handle exception" }
            assertException(
                exception, ExceptionMetadata(
                    "java.lang.IllegalStateException: Request failures mid-transfer",
                    ExceptionMetadata(
                        "Request failures mid-transfer",
                        ExceptionMetadata(
                            "CANCELLED: $STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST"
                        )
                    )
                )
            )
        }
    }

    private fun assertInterruptedSync(exception: ExecutionException) {
        K_LOGGER.error(exception) { "Handle exception" }
        assertException(
            exception, ExceptionMetadata(
                "java.lang.RuntimeException: Can not execute GRPC blocking request",
                ExceptionMetadata(
                    "Can not execute GRPC blocking request",
                    suspended = listOf(
                        ExceptionMetadata(
                            "UNAVAILABLE: io exception",
                            ExceptionMetadata(
                                "Connection refused: localhost/127.0.0.1:8080",
                                ExceptionMetadata(
                                    "Connection refused"
                                )
                            ),
                        ),
                        ExceptionMetadata(
                            "sleep interrupted"
                        ),
                    )
                )
            )
        )
    }

    private fun assertDeadlineExceededSync(exception: ExecutionException) {
        K_LOGGER.error(exception) { "Handle exception" }
        assertException(
            exception, ExceptionMetadata(
                "java.lang.RuntimeException: Can not execute GRPC blocking request",
                ExceptionMetadata(
                    "Can not execute GRPC blocking request",
                    suspended = listOf(
                        ExceptionMetadata(
                            "UNAVAILABLE: io exception",
                            ExceptionMetadata(
                                "Connection refused: localhost/127.0.0.1:8080",
                                ExceptionMetadata(
                                    "Connection refused"
                                )
                            ),
                        ),
                        ExceptionMetadata(
                            "DEADLINE_EXCEEDED: context timed out",
                            ExceptionMetadata(
                                "context timed out",
                            )
                        ),
                    )
                )
            )
        )
    }

    private fun assertCanceledSync(exception: ExecutionException) {
        K_LOGGER.error(exception) { "Handle exception" }
        assertException(
            exception, ExceptionMetadata(
                "java.lang.RuntimeException: Can not execute GRPC blocking request",
                ExceptionMetadata(
                    "Can not execute GRPC blocking request",
                    suspended = listOf(
                        ExceptionMetadata(
                            "UNAVAILABLE: io exception",
                            ExceptionMetadata(
                                "Connection refused: localhost/127.0.0.1:8080",
                                ExceptionMetadata(
                                    "Connection refused"
                                )
                            ),
                        ),
                        ExceptionMetadata(
                            "CANCELLED: Context cancelled",
                            ExceptionMetadata(
                                CANCEL_REASON,
                            )
                        ),
                    )
                )
            )
        )
    }

    private fun createRequest(): Request? = Request.newBuilder().setSeq(1).build()

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }

        private const val SERVER_PORT = 8080
        private const val RETRY_TIMEOUT = 1_000L
        private inline fun Server.execAndClose(force: Boolean = false, func: Server.() -> Unit = { }) {
            try {
                val startTime = Instant.now()
                func()
                K_LOGGER.info { "Function duration: ${Duration.between(startTime, Instant.now()).toMillis()} ms" }
            } finally {
                if (force) {
                    shutdownNow()
                } else {
                    shutdown()
                    if (!awaitTermination(5, TimeUnit.SECONDS)) {
                        shutdownNow()
                        error("'Server' can't be closed")
                    }
                }
            }
        }

        private fun assertException(
            exception: Throwable,
            exceptionMetadata: ExceptionMetadata,
            path: List<String?> = emptyList()
        ) {
            assertEquals(
                exceptionMetadata.message,
                exception.message,
                "Message for exception: $exception, path: ${path.printAsStackTrace()}"
            )
            exceptionMetadata.suspended?.let { suspendMetadataList ->
                assertEquals(
                    suspendMetadataList.size,
                    exception.suppressed.size,
                    "Suspended for exception: $exception, path: ${path.printAsStackTrace()}"
                )
                suspendMetadataList.forEachIndexed { index, suspendMetadata ->
                    assertException(
                        exception.suppressed[index],
                        suspendMetadata,
                        path.plus(listOf(exception.message, "[$index]"))
                    )
                }
            } ?: assertEquals(
                0,
                exception.suppressed.size,
                "Suspended for exception: $exception, path: ${path.printAsStackTrace()}"
            )
            exceptionMetadata.cause?.let { causeMetadata ->
                assertNotNull(exception.cause, "Cause for exception: $exception, path: ${path.printAsStackTrace()}")
                    .also { assertException(it, causeMetadata, path.plus(exception.message)) }
            } ?: assertNull(exception.cause, "Cause for exception: $exception, path: ${path.printAsStackTrace()}")
        }

        private fun List<String?>.printAsStackTrace() = asSequence()
            .joinToString(separator = "\n -> ", prefix = "\n -> ")

        private fun ExecutorService.shutdownGracefully() {
            shutdown()
            if (!awaitTermination(1, TimeUnit.SECONDS)) {
                shutdownNow()
                error("'Executor' can't be stopped")
            }
        }

        internal class ExceptionMetadata(
            val message: String? = null,
            val cause: ExceptionMetadata? = null,
            val suspended: List<ExceptionMetadata>? = null
        )

        internal class Baton(
            private val name: String
        ) {
            private val queue = ArrayBlockingQueue<Any>(1).apply { put(Any()) }

            fun giveAndGet(giveComment: String = "", getComment: String = "") {
                give(giveComment)
                get(getComment)
            }

            fun give(comment: String = "") {
                K_LOGGER.info { "'$name' baton is giving by [${Thread.currentThread().name}] - $comment" }
                queue.put(Any())
                K_LOGGER.info { "'$name' baton is given by [${Thread.currentThread().name}] - $comment" }
            }

            fun get(comment: String = "") {
                K_LOGGER.info { "'$name' baton is getting by [${Thread.currentThread().name}] - $comment" }
                queue.poll()
                K_LOGGER.info { "'$name' baton is got by [${Thread.currentThread().name}] - $comment" }
            }
        }

        private class TestServiceHandler(
            private val complete: Boolean = true,
            private val responseBaton: Baton? = null,
        ) : TestImplBase() {
            private val sequence = AtomicInteger()
            override fun singleRequestSingleResponse(request: Request, responseObserver: StreamObserver<Response>) {
                responseObserver.onNext(Response.newBuilder().apply {
                    origSeq = request.seq
                    seq = sequence.incrementAndGet()
                }.build())

                responseBaton?.let {
                    Thread.sleep(1_000)
                    it.give("response sent")
                }

                if (complete) {
                    responseObserver.onCompleted()
                }
            }

            override fun singleRequestMultipleResponse(request: Request, responseObserver: StreamObserver<Response>) {
                repeat(2) {
                    responseObserver.onNext(Response.newBuilder().apply {
                        origSeq = request.seq
                        seq = sequence.incrementAndGet()
                    }.build())
                }

                responseBaton?.let {
                    Thread.sleep(1_000)
                    it.give("response sent")
                }

                if (complete) {
                    responseObserver.onCompleted()
                }
            }
        }
    }
}
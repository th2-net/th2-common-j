/*
 *  Copyright 2023-2025 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.service.AbstractGrpcService.MID_TRANSFER_FAILURE_EXCEPTION_MESSAGE
import com.exactpro.th2.service.AbstractGrpcService.ROOT_RETRY_SYNC_EXCEPTION_MESSAGE
import com.exactpro.th2.service.AbstractGrpcService.STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.Context
import io.grpc.Deadline
import io.grpc.Server
import io.grpc.stub.StreamObserver
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.AfterTestExecutionCallback
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.ExtensionContext
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.timeout
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.testcontainers.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

private const val CANCEL_REASON = "test request is canceled"

@ExtendWith(DefaultGrpcRouterTest.ExecutionListener::class)
@IntegrationTest
internal class DefaultGrpcRouterTest {
    /**
     * Listener adds additional logging to help understanding from the stdout where test starts and finishes
     */
    internal class ExecutionListener : BeforeTestExecutionCallback, AfterTestExecutionCallback {
        private val logger = KotlinLogging.logger { }
        override fun beforeTestExecution(ctx: ExtensionContext) {
            logger.info { "Execution for test '${ctx.testMethod.map { it.name }.orElse("unknown")}' started" }
        }

        override fun afterTestExecution(ctx: ExtensionContext) {
            logger.info { "Execution for test '${ctx.testMethod.map { it.name }.orElse("unknown")}' is finished" }
        }

    }

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
                        ROOT_RETRY_SYNC_EXCEPTION_MESSAGE,
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

        @Disabled("this test isn't relevant for async request")
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
                    "java.lang.IllegalStateException: $MID_TRANSFER_FAILURE_EXCEPTION_MESSAGE",
                    ExceptionMetadata(
                        MID_TRANSFER_FAILURE_EXCEPTION_MESSAGE,
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
                    "java.lang.IllegalStateException: $MID_TRANSFER_FAILURE_EXCEPTION_MESSAGE",
                    ExceptionMetadata(
                        MID_TRANSFER_FAILURE_EXCEPTION_MESSAGE,
                        ExceptionMetadata(
                            "CANCELLED: $STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST"
                        )
                    )
                )
            )
        }
    }

    @Nested
    inner class SingleRequestMultipleResponseAsyncTest : AbstractGrpcRouterTest() {
        @Test
        override fun general() {
            val streamObserver = mock<StreamObserver<Response>> { }
            createServer().execAndClose(true) {
                createClientAsync().singleRequestMultipleResponse(createRequest(), streamObserver)

                val captor = argumentCaptor<Response> { }
                verify(streamObserver, timeout(60 * 1_000)).onCompleted()
                verify(streamObserver, times(2)).onNext(captor.capture())
                verifyNoMoreInteractions(streamObserver)

                assertEquals(2, captor.allValues.size)
                captor.allValues.forEachIndexed { index, response ->
                    assertEquals(index + 1, response.seq)
                    assertEquals(1, response.origSeq)
                }
            }
        }

        @Test
        override fun `delayed server start`() {
            val streamObserver = mock<StreamObserver<Response>> { }
            createClientAsync().singleRequestMultipleResponse(createRequest(), streamObserver)

            Thread.sleep(RETRY_TIMEOUT / 2)

            createServer().execAndClose {
                val captor = argumentCaptor<Response> { }
                verify(streamObserver, timeout(60 * 1_000)).onCompleted()
                verify(streamObserver, times(2)).onNext(captor.capture())
                verifyNoMoreInteractions(streamObserver)

                assertEquals(2, captor.allValues.size)
                captor.allValues.forEachIndexed { index, response ->
                    assertEquals(index + 1, response.seq)
                    assertEquals(1, response.origSeq)
                }
            }
        }

        @Test
        override fun `cancel retry request`() {
            val grpcContext = Context.current().withCancellation()

            val streamObserver = mock<StreamObserver<Response>> { }
            grpcContext.call {
                createClientAsync().singleRequestMultipleResponse(createRequest(), streamObserver)
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
                createClientAsync().singleRequestMultipleResponse(createRequest(), streamObserver)
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

        @Disabled("this test isn't relevant for async request")
        @Test
        override fun `interrupt thread during retry request`() {
            // this test isn't relevant for async request
        }

        @Test
        override fun `server terminated intermediate session (retry false)`() {
            val exception = `server terminated intermediate session`(false)
            K_LOGGER.error(exception) { "Handle exception" }

            assertException(
                exception, ExceptionMetadata(
                    "CANCELLED: $STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST",
                )
            )
        }

        @Test
        override fun `server terminated intermediate session (retry true)`() {
            val exception = `server terminated intermediate session`(true)
            K_LOGGER.error(exception) { "Handle exception" }

            assertException(
                exception, ExceptionMetadata(
                    MID_TRANSFER_FAILURE_EXCEPTION_MESSAGE,
                    suspended = listOf(
                        ExceptionMetadata(
                            "CANCELLED: $STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST",
                        )
                    )
                )
            )
        }

        private fun `server terminated intermediate session`(retryInterruptedTransaction: Boolean): Throwable {
            val streamObserver = mock<StreamObserver<Response>> { }
            val handlerBaton = Baton("handler")

            createServer(completeResponse = false, handlerBaton = handlerBaton).execAndClose(true) {
                createClientAsync(retryInterruptedTransaction = retryInterruptedTransaction)
                    .singleRequestMultipleResponse(createRequest(), streamObserver)

                handlerBaton.get("wait response sent")
                Thread.sleep(RETRY_TIMEOUT / 2)
            }

            val onErrorCaptor = argumentCaptor<Throwable> { }
            verify(streamObserver, timeout(60 * 1_000)).onError(onErrorCaptor.capture())
            val onNextCaptor = argumentCaptor<Response> { }
            verify(streamObserver, times(2)).onNext(onNextCaptor.capture())
            verifyNoMoreInteractions(streamObserver)

            assertEquals(1, onErrorCaptor.allValues.size)
            assertEquals(2, onNextCaptor.allValues.size)
            onNextCaptor.allValues.forEachIndexed { index, response ->
                assertEquals(index + 1, response.seq)
                assertEquals(1, response.origSeq)
            }

            return onErrorCaptor.firstValue
        }
    }

    private fun assertInterruptedSync(exception: ExecutionException) {
        K_LOGGER.error(exception) { "Handle exception" }
        assertException(
            exception, ExceptionMetadata(
                "java.lang.RuntimeException: Can not execute GRPC blocking request",
                ExceptionMetadata(
                    ROOT_RETRY_SYNC_EXCEPTION_MESSAGE,
                    suspended = listOf(
                        ExceptionMetadata(
                            "UNAVAILABLE: io exception",
                            ExceptionMetadata(
                                "Connection refused: localhost/",
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
                    ROOT_RETRY_SYNC_EXCEPTION_MESSAGE,
                    suspended = listOf(
                        ExceptionMetadata(
                            "UNAVAILABLE: io exception",
                            ExceptionMetadata(
                                "Connection refused: localhost/",
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
                    ROOT_RETRY_SYNC_EXCEPTION_MESSAGE,
                    suspended = listOf(
                        ExceptionMetadata(
                            "UNAVAILABLE: io exception",
                            ExceptionMetadata(
                                "Connection refused: localhost/",
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
                    if (!awaitTermination(60, TimeUnit.SECONDS)) {
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
            val expectedMessage = exceptionMetadata.message
            assertTrue(
                exception.message == expectedMessage || exception.message?.startsWith(
                    expectedMessage ?: "null"
                ) == true,
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
            if (!awaitTermination(30, TimeUnit.SECONDS)) {
                shutdownNow()
                error("'Executor' can't be stopped")
            }
        }

        internal class ExceptionMetadata(
            val message: String? = null,
            val cause: ExceptionMetadata? = null,
            val suspended: List<ExceptionMetadata>? = null
        )

        /**
         * Baton class can help to synchronize two threads (only **two**).
         *
         * Baton class was migrated from using queue with size 1 to lock and conditions for synchronization.
         *
         * The implementation with queue did not provide guarantees that the same thread won't get the permit and put it back
         * while another thread was still waiting for a free space in the queue.
         *
         * Using lock and conditions guarantees that the permit won't be given unless somebody is waiting for that permit.
         * And vise-versa, nobody can get a permit unless somebody tries to put the permit
         */
        internal class Baton(
            private val name: String
        ) {
            @Volatile
            private var permits = 0
            private val lock = ReentrantLock()
            private val givenCondition = lock.newCondition()
            private val getCondition = lock.newCondition()

            fun giveAndGet(giveComment: String = "", getComment: String = "") {
                give(giveComment)
                get(getComment)
            }

            fun give(comment: String = "") {
                K_LOGGER.info { "'$name' baton is giving by [${Thread.currentThread().name}] - $comment" }
                lock.withLock {
                    if (permits == 0) {
                        getCondition.await()
                    }
                    permits += 1
                    givenCondition.signal()
                }
                K_LOGGER.info { "'$name' baton is given by [${Thread.currentThread().name}] - $comment" }
            }

            fun get(comment: String = "") {
                K_LOGGER.info { "'$name' baton is getting by [${Thread.currentThread().name}] - $comment" }
                lock.withLock {
                    getCondition.signal()
                    permits -= 1
                    if (permits < 0) {
                        givenCondition.await()
                    }
                }
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
                    it.give("response sent")
                    Thread.sleep(1_000)
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
                    it.give("response sent")
                    Thread.sleep(1_000)
                }

                if (complete) {
                    responseObserver.onCompleted()
                }
            }
        }
    }
}
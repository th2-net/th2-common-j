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
import com.exactpro.th2.common.test.grpc.Request
import com.exactpro.th2.common.test.grpc.Response
import com.exactpro.th2.common.test.grpc.TestGrpc.TestImplBase
import com.exactpro.th2.common.test.grpc.TestService
import com.exactpro.th2.service.AbstractGrpcService.STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST
import io.grpc.Context
import io.grpc.Deadline
import io.grpc.Server
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.testcontainers.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@IntegrationTest
internal class DefaultGrpcRouterTest {

    private val grpcRouterClient = DefaultGrpcRouter()
    private val grpcRouterServer = DefaultGrpcRouter()
    private val executor = Executors.newSingleThreadExecutor(
        ThreadFactoryBuilder().setNameFormat("test-%d").build()
    )
    private val deadlineExecutor = Executors.newSingleThreadScheduledExecutor(
        ThreadFactoryBuilder().setNameFormat("test-deadline-%d").build()
    )

    @AfterEach
    fun afterEach() {
        grpcRouterServer.close()
        grpcRouterClient.close()
        executor.shutdownGracefully()
        deadlineExecutor.shutdownGracefully()
    }

    @Test
    fun `single request single response`() {
        createServer().use(true) {
            val response = executor.submit<Response> {
                return@submit createClient().singleRequestSingleResponse(Request.newBuilder().setSeq(1).build())
            }.get(1, TimeUnit.MINUTES)

            assertEquals(1, response.seq)
            assertEquals(1, response.origSeq)
        }
    }

    @Test
    fun `single request single response - delayed server start`() {
        val clientServerBaton = Baton("client-server")
        val future = executor.submit<Response> {
            clientServerBaton.give("client thread started")
            return@submit createClient().singleRequestSingleResponse(Request.newBuilder().setSeq(1).build())
        }

        clientServerBaton.get("wait client thread start")
        Thread.sleep(RETRY_TIMEOUT * 2)

        createServer().use {
            val response = future.get(1, TimeUnit.MINUTES)
            assertEquals(1, response.seq)
            assertEquals(1, response.origSeq)
        }
    }

    @Test
    fun `single request single response - server is terminated in intermediate session (retry false)`() {
        val clientServerBaton = Baton("client-server")
        val future = executor.submit<Response> {
            clientServerBaton.giveAndGet("client thread started", "wait server start")
            createClient().singleRequestSingleResponse(Request.newBuilder().setSeq(1).build())
        }

        val handlerBaton = Baton("handler")

        createServer(completeResponse = false, handlerBaton = handlerBaton).use(true) {
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
    fun `single request single response - server is terminated in intermediate session (retry true)`() {
        val clientServerBaton = Baton("client-server")
        val future = executor.submit<Response> {
            clientServerBaton.giveAndGet("client thread started", "wait server start")
            createClient(retryInterruptedTransaction = true).singleRequestSingleResponse(
                Request.newBuilder().setSeq(1).build()
            )
        }

        val handlerBaton = Baton("handler")

        createServer(completeResponse = false, handlerBaton = handlerBaton).use(true) {
            clientServerBaton.get("wait client thread start")
            clientServerBaton.give("server started")
            handlerBaton.get("wait response sent")
            Thread.sleep(RETRY_TIMEOUT / 2)
        }

        assertFalse(future.isDone)

        DefaultGrpcRouter().use { grpcRouter ->
            createServer(grpcRouter = grpcRouter).use {
                val response = future.get(1, TimeUnit.MINUTES)
                assertEquals(1, response.seq)
                assertEquals(1, response.origSeq)
            }
        }
    }

    @Test
    fun `single request single response - cancel request`() {
        val clientServerBaton = Baton("client-server")
        val grpcContext = AtomicReference<Context.CancellableContext>()

        val future = executor.submit<Response> {
            grpcContext.set(
                Context.current()
                    .withCancellation()
            )

            clientServerBaton.give("client thread started")
            grpcContext.get().call {
                createClient().singleRequestSingleResponse(Request.newBuilder().setSeq(1).build())
            }
        }

        clientServerBaton.get("wait client thread start")
        Thread.sleep(RETRY_TIMEOUT / 2)
        val cancelExceptionMessage = "test request is canceled"
        assertTrue(grpcContext.get().cancel(RuntimeException(cancelExceptionMessage)))

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
                                cancelExceptionMessage,
                            )
                        ),
                    )
                )
            )
        )
    }

    @Test
    fun `single request single response - deadline request`() {
        val clientServerBaton = Baton("client-server")

        val future = executor.submit<Response> {
            clientServerBaton.give("client thread started")
            Context.current()
                .withDeadline(Deadline.after(RETRY_TIMEOUT / 2, TimeUnit.MILLISECONDS), deadlineExecutor)
                .call {
                    createClient().singleRequestSingleResponse(Request.newBuilder().setSeq(1).build())
                }
        }

        clientServerBaton.get("wait client thread start")

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

    @Test
    fun `single request single response - interrupt thread`() {
        val clientServerBaton = Baton("client-server")

        val future = executor.submit<Response> {
            clientServerBaton.give("client thread started")
            createClient().singleRequestSingleResponse(Request.newBuilder().setSeq(1).build())
        }

        clientServerBaton.get("wait client thread start")
        Thread.sleep(RETRY_TIMEOUT / 2)

        assertEquals(0, executor.shutdownNow().size)

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


    @Test
    fun `single request multiple response`() {
        createServer().use {
            val responses = createClient().singleRequestMultipleResponse(Request.newBuilder().setSeq(1).build())
                .asSequence().toList()

            assertEquals(2, responses.size)
            responses.forEachIndexed { index, response ->
                assertEquals(index + 1, response.seq)
                assertEquals(1, response.origSeq)
            }
        }
    }

    @Test
    fun `single request multiple response - delayed server start`() {
        val iterator = createClient().singleRequestMultipleResponse(Request.newBuilder().setSeq(1).build())
        val exception = assertThrows<StatusRuntimeException> {
            // FIXME: gRPC router should retry to resend request when server isn't available.
            //  iterator can throw exception when server disappears in the intermediate of response retransmission
            //  and retryInterruptedTransaction false
            iterator.hasNext()
        }
        K_LOGGER.error(exception) { "Handle exception" }
        assertException(
            exception, ExceptionMetadata(
                "UNAVAILABLE: io exception",
                cause = ExceptionMetadata(
                    "Connection refused: localhost/127.0.0.1:8080",
                    cause = ExceptionMetadata(
                        "Connection refused"
                    )
                )
            )
        )
    }

    @Test
    fun `single request multiple response - server terminated intermediate session (retry false)`() {
        val clientServerBaton = Baton("client-server")
        val future = executor.submit<List<Response>> {
            clientServerBaton.giveAndGet("client thread started", "wait server start")
            createClient().singleRequestMultipleResponse(Request.newBuilder().setSeq(1).build())
                .asSequence().toList()
        }

        val handlerBaton = Baton("handler")

        createServer(completeResponse = true, handlerBaton = handlerBaton).use(true) {
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
                "io.grpc.StatusRuntimeException: CANCELLED: $STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST",
                ExceptionMetadata(
                    "CANCELLED: $STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST",
                )
            )
        )
    }

    @Test
    fun `single request multiple response - server terminated intermediate session (retry true)`() {
        val clientServerBaton = Baton("client-server")
        val future = executor.submit<List<Response>> {
            clientServerBaton.giveAndGet("client thread started", "wait server start")
            // FIXME: gRPC router should retry to resend request when server is terminated intermediate handling.
            //  iterator can throw exception when server disappears in the intermediate of response retransmission
            //  and retryInterruptedTransaction false
            createClient(retryInterruptedTransaction = true).singleRequestMultipleResponse(
                Request.newBuilder().setSeq(1).build()
            ).asSequence().toList()
        }

        val handlerBaton = Baton("handler")

        createServer(completeResponse = true, handlerBaton = handlerBaton).use(true) {
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
                "io.grpc.StatusRuntimeException: CANCELLED: $STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST",
                ExceptionMetadata(
                    "CANCELLED: $STATUS_DESCRIPTION_OF_INTERRUPTED_REQUEST",
                )
            )
        )
    }

    private fun createClient(
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

    private fun createServer(
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

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }

        private const val SERVER_PORT = 8080
        private const val RETRY_TIMEOUT = 1_000L
        private inline fun Server.use(force: Boolean = false, func: Server.() -> Unit) {
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

        private class ExceptionMetadata(
            val message: String? = null,
            val cause: ExceptionMetadata? = null,
            val suspended: List<ExceptionMetadata>? = null
        )

        private class Baton(
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
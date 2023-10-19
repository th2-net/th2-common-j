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
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@IntegrationTest
internal class DefaultGrpcRouterTest {

    private val grpcRouterClient = DefaultGrpcRouter()
    private val grpcRouterServer = DefaultGrpcRouter()
    private val executor = Executors.newSingleThreadExecutor(
        ThreadFactoryBuilder().setNameFormat("test-%d").build()
    )

    @AfterEach
    fun afterEach() {
        grpcRouterServer.close()
        grpcRouterClient.close()
        executor.shutdown()
        if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            executor.shutdownNow()
            error("'Executor' can't be stopped")
        }
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
        Thread.sleep(1_000)

        createServer().use {
            val response = future.get(1, TimeUnit.MINUTES)
            assertEquals(1, response.seq)
            assertEquals(1, response.origSeq)
        }
    }

    @Test
    fun `single request single response - server terminated intermediate session`() {
        val clientServerBaton = Baton("client-server")
        val future = executor.submit<Response> {
            clientServerBaton.giveAndGet("client thread started", "wait server start")
//        FIXME: client should differ Interrupted request and Connection refused
//            return@submit assertThrows<Exception> {
                return@submit createClient().singleRequestSingleResponse(Request.newBuilder().setSeq(1).build())
//            }
        }

        clientServerBaton.get("wait client thread start")

        val handlerBaton = Baton("handler")

        createServer(completeResponse = false, handlerBaton = handlerBaton).use(true) {
            clientServerBaton.give("server started")
            handlerBaton.get("wait response sent")
        }

//        FIXME: client should differ Interrupted request and Connection refused
//        val exception = future.get(1, TimeUnit.MINUTES)
//        assertEquals("", exception.message)

        DefaultGrpcRouter().use { grpcRouter ->
            createServer(grpcRouter = grpcRouter).use {
                val response = future.get(1, TimeUnit.MINUTES)
                assertEquals(1, response.seq)
                assertEquals(1, response.origSeq)
            }
        }
    }

    @Test
    fun `single request multiple response`() {
        createServer().use {
            val responses = executor.submit<List<Response>> {
                createClient().singleRequestMultipleResponse(Request.newBuilder().setSeq(1).build()).asSequence().toList()
            }.get(1, TimeUnit.SECONDS)

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
            iterator.hasNext()
        }
        exception.printStackTrace()
        assertEquals("UNAVAILABLE: io exception", exception.message)
        var cause = exception.cause
        assertNotNull(cause)
        assertEquals("Connection refused: localhost/127.0.0.1:8080", cause.message)
        cause = cause.cause
        assertNotNull(cause)
        assertEquals("Connection refused", cause.message)
        assertNull(cause.cause)
    }

    @Test
    fun `single request multiple response - server terminated intermediate session`() {
        val clientServerBaton = Baton("client-server")
        val future = executor.submit<StatusRuntimeException> {
            clientServerBaton.giveAndGet("client thread started", "wait server start")
            return@submit assertThrows<StatusRuntimeException> {
                createClient().singleRequestMultipleResponse(Request.newBuilder().setSeq(1).build()).asSequence().toList()
            }
        }

        clientServerBaton.get("wait client thread start")

        val handlerBaton = Baton("handler")

        createServer(completeResponse = true, handlerBaton = handlerBaton).use(true) {
            clientServerBaton.give("server started")
            handlerBaton.get("wait response sent")
        }

        val exception = future.get(1, TimeUnit.MINUTES)
        exception.printStackTrace()
        assertEquals("UNAVAILABLE: io exception", exception.message)
        var cause = exception.cause
        assertNotNull(cause)
        assertEquals("Connection refused: localhost/127.0.0.1:8080", cause.message)
        cause = cause.cause
        assertNotNull(cause)
        assertEquals("Connection refused", cause.message)
        assertNull(cause.cause)
    }

    private fun createClient(
        configuration: GrpcServiceConfiguration = GrpcServiceConfiguration(
            RobinRoutingStrategy().apply { init(GrpcRawRobinStrategy(listOf("endpoint"))) },
            TestService::class.java,
            mapOf("endpoint" to GrpcEndpointConfiguration("localhost", SERVER_PORT))
        )
    ): TestService {
        grpcRouterClient.init(
            GrpcConfiguration(services = mapOf("test" to configuration)),
            GrpcRouterConfiguration(
                retryConfiguration = GrpcRetryConfiguration(
                    Int.MAX_VALUE,
                    500,
                    500
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
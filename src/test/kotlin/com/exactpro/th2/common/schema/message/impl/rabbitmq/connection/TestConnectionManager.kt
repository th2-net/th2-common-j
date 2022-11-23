/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection

import com.exactpro.th2.common.annotations.IntegrationTest
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.rabbitmq.client.BuiltinExchangeType
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

private val LOGGER = KotlinLogging.logger { }
private val RABBIT_LOGGER = KotlinLogging.logger("RabbitMQ-container")
private const val DEFAULT_AMQP_PORT = 5672


private const val RABBITMQ_DATABASE_PATH = "/var/lib/rabbitmq/mnesia/"
private const val BUILD_DIR = "build"
private const val INTEGRATION_TEST_DIR = "integration-test"
private const val LOCAL_PATH_FOR_RABBITMQ = "${BUILD_DIR}/$INTEGRATION_TEST_DIR/rabbitmq/"

@IntegrationTest
class TestConnectionManager {

    @Test
    fun `connection manager reports unacked messages when confirmation timeout elapsed`() {
        val routingKey = "routingKey"
        val queueName = "queue"
        val exchange = "test-exchange"
        val prefetchCount = 10
        createRabbitMqContainer(
            exchange,
            mapOf(
                queueName bind routingKey
            ),
        ) { container ->
            LOGGER.info { "Started with port ${container.amqpPort}" }
            val queue = ArrayBlockingQueue<ManualAckDeliveryCallback.Confirmation>(prefetchCount)
            val countDown = CountDownLatch(prefetchCount)
            val confirmationTimeout = Duration.ofSeconds(1)
            ConnectionManager(
                container.createConfig(),
                ConnectionManagerConfiguration(
                    subscriberName = "test",
                    prefetchCount = prefetchCount,
                    confirmationTimeout = confirmationTimeout,
                ),
            ) {
                LOGGER.error { "Fatal connection problem" }
            }.use { manager ->
                manager.basicConsume(queueName, { _, delivery, ack ->
                    LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                    queue += ack
                    countDown.countDown()
                }) {
                    LOGGER.info { "Canceled $it" }
                }

                repeat(prefetchCount + 1) { index ->
                    manager.basicPublish(exchange, routingKey, null, "Hello $index".toByteArray(Charsets.UTF_8))
                }

                Assertions.assertTrue(countDown.await(1L, TimeUnit.SECONDS)) { "Not all messages were received: ${countDown.count}" }

                Assertions.assertTrue(manager.isAlive) { "Manager should still be alive" }
                Assertions.assertTrue(manager.isReady) { "Manager should be ready until the confirmation timeout expires" }

                Thread.sleep(confirmationTimeout.toMillis() + 100/*just in case*/) // wait for confirmation timeout

                Assertions.assertTrue(manager.isAlive) { "Manager should still be alive" }
                Assertions.assertFalse(manager.isReady) { "Manager should not be ready" }

                queue.poll().confirm()

                Assertions.assertTrue(manager.isAlive) { "Manager should still be alive" }
                Assertions.assertTrue(manager.isReady) { "Manager should be ready" }

                val receivedData = generateSequence { queue.poll(10L, TimeUnit.MILLISECONDS) }
                    .onEach(ManualAckDeliveryCallback.Confirmation::confirm)
                    .count()
                Assertions.assertEquals(prefetchCount, receivedData) { "Unexpected number of messages received" }
            }
        }
    }

    private fun RabbitMQContainer.createConfig(): RabbitMQConfiguration = RabbitMQConfiguration(
        host = host,
        vHost = "",
        port = amqpPort,
        username = adminUsername,
        password = adminPassword,
    )

    private fun createRabbitMqContainer(
        exchange: String,
        queuesToRoutingKey: Map<String, List<String>>,
        persistent: Boolean = false,
        fixedAmqpPort: Boolean = false,
        action: (RabbitMQContainer) -> Unit,
    ) {
        val result = RabbitMQContainer(DockerImageName.parse("rabbitmq:3.8-management-alpine"))
            .withLogConsumer {
                with(RABBIT_LOGGER) {
                    when (it.type) {
                        OutputFrame.OutputType.STDOUT, OutputFrame.OutputType.END -> info { it.utf8String }
                        OutputFrame.OutputType.STDERR -> error { it.utf8String }
                        null -> {}
                    }
                }
            }
            .withExchange(exchange, BuiltinExchangeType.DIRECT.type, false, false, true, emptyMap())
            .withCreateContainerCmdModifier {
                it.withHostName("test-rabbitmq") // rabbitmq uses hostname for its database name, so it should be constant
            }.apply {
                if (fixedAmqpPort) {
                    portBindings = listOf("$DEFAULT_AMQP_PORT:$DEFAULT_AMQP_PORT")
                }
                for ((queue, routingKeys) in queuesToRoutingKey) {
                    withQueue(queue)
                    routingKeys.forEach { routingKey ->
                        withBinding(exchange, queue, emptyMap(), routingKey, "queue")
                    }
                }
                if (persistent) {
                    bindRabbitMqDatabase()
                }
                start()
            }.runCatching { use(action) }

        if (persistent) {
            runCatching { cleanup() }.onFailure { LOGGER.error(it) { "Cannot cleanup" } }
        }

        result.getOrThrow()
    }

    private fun cleanup() {
        val cleanupDir = "/home/cleanup"
        GenericContainer(DockerImageName.parse("rabbitmq:3.8-management-alpine"))
            .withFileSystemBind(BUILD_DIR, cleanupDir, BindMode.READ_WRITE)
            .withCreateContainerCmdModifier {
                it.withEntrypoint()
                    .withCmd("/bin/rm", "-r", "$cleanupDir/$INTEGRATION_TEST_DIR")
            }.use {
                it.start()
            }
    }

    private fun <T : org.testcontainers.containers.GenericContainer<T>> T.bindRabbitMqDatabase(): T {
//        Files.createDirectories(Path.of(LOCAL_PATH_FOR_RABBITMQ))
        return withFileSystemBind(LOCAL_PATH_FOR_RABBITMQ, RABBITMQ_DATABASE_PATH, BindMode.READ_WRITE)
    }

    private class GenericContainer(
        name: DockerImageName,
    ) : org.testcontainers.containers.GenericContainer<GenericContainer>(name)

    private infix fun <A, B> A.bind(value: B): Pair<A, List<B>> {
        return this to listOf(value)
    }
}
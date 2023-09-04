/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.schema.message.impl.rabbitmq

import com.exactpro.th2.common.annotations.IntegrationTest
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.context.DefaultMessageRouterContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.rabbitmq.client.BuiltinExchangeType
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions.assertNull
import org.mockito.kotlin.mock
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@IntegrationTest
class AbstractRabbitRouterIntegrationTest {

    @Test
    fun `receive unconfirmed message after resubscribe`() {
        RabbitMQContainer(DockerImageName.parse(RABBITMQ_MANAGEMENT_ALPINE))
            .withExchange(EXCHANGE, BuiltinExchangeType.DIRECT.type, false, false, true, emptyMap())
            .withQueue(QUEUE_NAME, false, true, emptyMap())
            .withBinding(
                EXCHANGE,
                QUEUE_NAME, emptyMap(),
                ROUTING_KEY, "queue")
            .use { rabbitMQContainer ->
                rabbitMQContainer.start()
                K_LOGGER.info { "Started with port ${rabbitMQContainer.amqpPort}, rest ${rabbitMQContainer.httpUrl} ${rabbitMQContainer.adminUsername} ${rabbitMQContainer.adminPassword} " }

                createConnectionManager(rabbitMQContainer).use { firstManager ->
                    createRouter(firstManager).use { firstRouter ->
                        val messageA = "test-message-a"
                        val messageB = "test-message-b"
                        val messageC = "test-message-c"
                        val messageD = "test-message-d"

                        val queue = ArrayBlockingQueue<Delivery>(4)

                        firstRouter.send(messageA)
                        firstRouter.send(messageB)
                        firstRouter.send(messageC)
                        firstRouter.send(messageD)

                        connectAndCheck(rabbitMQContainer, queue, listOf(
                            Expectation(messageA, 1, false, ManualAckDeliveryCallback.Confirmation::confirm),
                            Expectation(messageB, 2, false, ManualAckDeliveryCallback.Confirmation::reject),
                            Expectation(messageC, 3, false) { },
                            Expectation(messageD, 4, false) { },
                        ))

                        connectAndCheck(rabbitMQContainer, queue, listOf(
                            Expectation(messageC, 1, true, ManualAckDeliveryCallback.Confirmation::confirm),
                            Expectation(messageD, 2, true) { },
                        ))

                        connectAndCheck(rabbitMQContainer, queue, listOf(
                            Expectation(messageD, 1, true, ManualAckDeliveryCallback.Confirmation::reject),
                        ))

                        connectAndCheck(rabbitMQContainer, queue, emptyList())
                    }
                }
            }
    }

    private fun connectAndCheck(
        rabbitMQContainer: RabbitMQContainer,
        queue: ArrayBlockingQueue<Delivery>,
        expectations: List<Expectation>,
    ) {
        createConnectionManager(rabbitMQContainer).use { manager ->
            createRouter(manager).use { router ->
                val monitor = router.subscribeWithManualAck({ deliveryMetadata, message, confirmation ->
                    queue.put(Delivery(message, deliveryMetadata.deliveryTag, deliveryMetadata.isRedelivered, confirmation))
                })

                try {
                    expectations.forEach { expectation ->
                        val delivery = assertNotNull(queue.poll(1, TimeUnit.SECONDS))
                        assertEquals(expectation.message, delivery.message, "Message")
                        assertEquals(expectation.deliveryTag, delivery.deliveryTag, "Delivery tag")
                        assertEquals(expectation.redelivery, delivery.redelivery, "Redelivery flag")
                        expectation.action.invoke(delivery.confirmation)
                    }

                    assertNull(queue.poll(1, TimeUnit.SECONDS))
                } finally {
                    monitor.unsubscribe()
                }
            }

            createRouter(manager).use { router ->
                val monitor = router.subscribeWithManualAck({ deliveryMetadata, message, confirmation ->
                    queue.put(Delivery(message, deliveryMetadata.deliveryTag, deliveryMetadata.isRedelivered, confirmation))
                })

                try {
                    // RabbitMQ doesn't resend messages after resubscribe using the same connection and channel
                    assertNull(queue.poll(1, TimeUnit.SECONDS))
                } finally {
                    monitor.unsubscribe()
                }
            }
        }
    }

    private fun createConnectionManager(
        rabbitMQContainer: RabbitMQContainer,
        prefetchCount: Int = DEFAULT_PREFETCH_COUNT,
        confirmationTimeout: Duration = DEFAULT_CONFIRMATION_TIMEOUT
    ) = ConnectionManager(
        RabbitMQConfiguration(
            host = rabbitMQContainer.host,
            vHost = "",
            port = rabbitMQContainer.amqpPort,
            username = rabbitMQContainer.adminUsername,
            password = rabbitMQContainer.adminPassword,
        ),
        ConnectionManagerConfiguration(
            subscriberName = "test",
            prefetchCount = prefetchCount,
            confirmationTimeout = confirmationTimeout,
        ),
    ) {
        K_LOGGER.error { "Fatal connection problem" }
    }

    private fun createRouter(connectionManager: ConnectionManager) = TestRouter()
        .apply {
            init(
                DefaultMessageRouterContext(
                    connectionManager,
                    mock { },
                    MessageRouterConfiguration(mapOf(
                        "test" to QueueConfiguration(
                            routingKey = ROUTING_KEY,
                            queue = "",
                            exchange = "test-exchange",
                            attributes = listOf("publish")
                        ),
                        "test1" to QueueConfiguration(
                            routingKey = "",
                            queue = QUEUE_NAME,
                            exchange = EXCHANGE,
                            attributes = listOf("subscribe")
                        ),
                    )),
                    BoxConfiguration()
                )
            )
        }
    
    companion object {
        private val K_LOGGER = KotlinLogging.logger { }

        private const val RABBITMQ_MANAGEMENT_ALPINE = "rabbitmq:3.11.2-management-alpine"
        private const val ROUTING_KEY = "routingKey"
        private const val QUEUE_NAME = "queue"
        private const val EXCHANGE = "test-exchange"

        private const val DEFAULT_PREFETCH_COUNT = 10
        private val DEFAULT_CONFIRMATION_TIMEOUT: Duration = Duration.ofSeconds(1)

        private class Expectation(val message: String, val deliveryTag: Long, val redelivery: Boolean, val action: ManualAckDeliveryCallback.Confirmation.() -> Unit)
        private class Delivery(val message: String, val deliveryTag: Long, val redelivery: Boolean, val confirmation: ManualAckDeliveryCallback.Confirmation)
    }
}
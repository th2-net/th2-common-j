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
import com.exactpro.th2.common.schema.message.ContainerConstants.Companion.DEFAULT_CONFIRMATION_TIMEOUT
import com.exactpro.th2.common.schema.message.ContainerConstants.Companion.DEFAULT_PREFETCH_COUNT
import com.exactpro.th2.common.schema.message.ContainerConstants.Companion.EXCHANGE
import com.exactpro.th2.common.schema.message.ContainerConstants.Companion.QUEUE_NAME
import com.exactpro.th2.common.schema.message.ContainerConstants.Companion.RABBITMQ_IMAGE_NAME
import com.exactpro.th2.common.schema.message.ContainerConstants.Companion.ROUTING_KEY
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.context.DefaultMessageRouterContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.exactpro.th2.common.schema.message.impl.rabbitmq.custom.RabbitCustomRouter
import com.rabbitmq.client.BuiltinExchangeType
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions.assertNull
import org.mockito.kotlin.mock
import org.testcontainers.containers.RabbitMQContainer
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
        RabbitMQContainer(RABBITMQ_IMAGE_NAME)
            .withExchange(EXCHANGE, BuiltinExchangeType.DIRECT.type, false, false, true, emptyMap())
            .withQueue(QUEUE_NAME, false, true, emptyMap())
            .withBinding(
                EXCHANGE,
                QUEUE_NAME, emptyMap(),
                ROUTING_KEY, "queue"
            )
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

                        connectAndCheck(
                            rabbitMQContainer, queue, listOf(
                                Expectation(messageA, false, ManualAckDeliveryCallback.Confirmation::confirm),
                                Expectation(messageB, false, ManualAckDeliveryCallback.Confirmation::reject),
                                Expectation(messageC, false) { },
                                Expectation(messageD, false) { },
                            )
                        )

                        connectAndCheck(
                            rabbitMQContainer, queue, listOf(
                                Expectation(messageC, true, ManualAckDeliveryCallback.Confirmation::confirm),
                                Expectation(messageD, true) { },
                            )
                        )

                        connectAndCheck(
                            rabbitMQContainer, queue, listOf(
                                Expectation(messageD, true, ManualAckDeliveryCallback.Confirmation::reject),
                            )
                        )

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
                    queue.put(
                        Delivery(
                            message,
                            deliveryMetadata.isRedelivered,
                            confirmation
                        )
                    )
                })

                try {
                    expectations.forEach { expectation ->
                        val delivery = assertNotNull(queue.poll(1, TimeUnit.SECONDS))
                        assertEquals(expectation.message, delivery.message, "Message")
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
                    queue.put(
                        Delivery(
                            message,
                            deliveryMetadata.isRedelivered,
                            confirmation
                        )
                    )
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
    )

    private fun createRouter(connectionManager: ConnectionManager) = RabbitCustomRouter(
        "test-custom-tag",
        arrayOf("test-label"),
        TestMessageConverter()
    ).apply {
        init(
            DefaultMessageRouterContext(
                connectionManager,
                mock { },
                MessageRouterConfiguration(
                    mapOf(
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
                    )
                ),
                BoxConfiguration()
            )
        )
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }

        private class Expectation(
            val message: String,
            val redelivery: Boolean,
            val action: ManualAckDeliveryCallback.Confirmation.() -> Unit
        )

        private class Delivery(
            val message: String,
            val redelivery: Boolean,
            val confirmation: ManualAckDeliveryCallback.Confirmation
        )
    }
}
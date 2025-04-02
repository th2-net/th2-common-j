/*
 * Copyright 2022-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.group

import com.exactpro.th2.common.annotations.IntegrationTest
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration
import com.exactpro.th2.common.schema.message.ContainerConstants.DEFAULT_CONFIRMATION_TIMEOUT
import com.exactpro.th2.common.schema.message.ContainerConstants.DEFAULT_PREFETCH_COUNT
import com.exactpro.th2.common.schema.message.ContainerConstants.EXCHANGE
import com.exactpro.th2.common.schema.message.ContainerConstants.QUEUE_NAME
import com.exactpro.th2.common.schema.message.ContainerConstants.RABBITMQ_IMAGE_NAME
import com.exactpro.th2.common.schema.message.ContainerConstants.ROUTING_KEY
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.impl.context.DefaultMessageRouterContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConsumeConnectionManager
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.PublishConnectionManager
import com.exactpro.th2.common.util.getRabbitMQConfiguration
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.rabbitmq.client.BuiltinExchangeType
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.testcontainers.containers.RabbitMQContainer
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue

@IntegrationTest
class IntegrationTestRabbitMessageGroupBatchRouter {
    private val channelChecker = Executors.newSingleThreadScheduledExecutor(ThreadFactoryBuilder().setNameFormat("channel-checker-%d").build())
    private val sharedExecutor = Executors.newFixedThreadPool(1, ThreadFactoryBuilder().setNameFormat("rabbitmq-shared-pool-%d").build())

    @Test
    fun `subscribe to exclusive queue`() {
        RabbitMQContainer(RABBITMQ_IMAGE_NAME)
            .use { rabbitMQContainer ->
                rabbitMQContainer.start()
                LOGGER.info { "Started with port ${rabbitMQContainer.amqpPort}" }

                createConsumeConnectionManager(rabbitMQContainer).use { firstConsumeManager ->
                    createPublishConnectionManager(rabbitMQContainer).use { firstPublishManager ->
                        createRouter(firstPublishManager, firstConsumeManager).use { firstRouter ->
                            createConsumeConnectionManager(rabbitMQContainer).use { secondConsumeManager ->
                                createPublishConnectionManager(rabbitMQContainer).use { secondPublishManager ->
                                    createRouter(secondPublishManager, secondConsumeManager).use { secondRouter ->
                                        val counter = CountDownLatch(1)
                                        val monitor = firstRouter.subscribeExclusive { _, _ -> counter.countDown() }
                                        try {
                                            secondRouter.sendExclusive(
                                                monitor.queue,
                                                MessageGroupBatch.getDefaultInstance()
                                            )
                                            assertTrue("Message is not received") { counter.await(1, TimeUnit.SECONDS) }
                                        } finally {
                                            monitor.unsubscribe()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
    }

    @Test
    fun `send receive message group batch`() {
        RabbitMQContainer(RABBITMQ_IMAGE_NAME)
            .withExchange(EXCHANGE, BuiltinExchangeType.DIRECT.type, false, false, true, emptyMap())
            .withQueue(QUEUE_NAME)
            .withBinding(EXCHANGE, QUEUE_NAME, emptyMap(), ROUTING_KEY, "queue")
            .use { rabbitMQContainer ->
                rabbitMQContainer.start()
                LOGGER.info { "Started with port ${rabbitMQContainer.amqpPort}" }

                createConsumeConnectionManager(rabbitMQContainer).use { firstConsumeManager ->
                    createPublishConnectionManager(rabbitMQContainer).use { firstPublishManager ->
                        createRouter(firstPublishManager, firstConsumeManager).use { firstRouter ->
                            createConsumeConnectionManager(rabbitMQContainer).use { secondConsumeManager ->
                                createPublishConnectionManager(rabbitMQContainer).use { secondPublishManager ->
                                    createRouter(secondPublishManager, secondConsumeManager).use { secondRouter ->
                                        val counter = CountDownLatch(1)
                                        val monitor = firstRouter.subscribeExclusive { _, _ -> counter.countDown() }
                                        try {

                                            secondRouter.sendExclusive(monitor.queue, MessageGroupBatch.getDefaultInstance())
                                            assertTrue("Message is not received") { counter.await(1, TimeUnit.SECONDS) }

                                        } finally {
                                            monitor.unsubscribe()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
    }

    private fun createRouter(publishConnectionManager: PublishConnectionManager, consumeConnectionManager: ConsumeConnectionManager) = RabbitMessageGroupBatchRouter()
        .apply {
            init(
                DefaultMessageRouterContext(
                    publishConnectionManager,
                    consumeConnectionManager,
                    mock { },
                    MessageRouterConfiguration(),
                    BoxConfiguration()
                )
            )
        }

    private fun getConnectionManagerConfiguration(prefetchCount: Int, confirmationTimeout: Duration) = ConnectionManagerConfiguration(
        subscriberName = "test",
        prefetchCount = prefetchCount,
        confirmationTimeout = confirmationTimeout
    )

    private fun createPublishConnectionManager(
        rabbitMQContainer: RabbitMQContainer,
        prefetchCount: Int = DEFAULT_PREFETCH_COUNT,
        confirmationTimeout: Duration = DEFAULT_CONFIRMATION_TIMEOUT
    ) = PublishConnectionManager(
        "test-publish-connection",
        getRabbitMQConfiguration(rabbitMQContainer),
        getConnectionManagerConfiguration(prefetchCount, confirmationTimeout),
        sharedExecutor,
        channelChecker
    )

    private fun createConsumeConnectionManager(
        rabbitMQContainer: RabbitMQContainer,
        prefetchCount: Int = DEFAULT_PREFETCH_COUNT,
        confirmationTimeout: Duration = DEFAULT_CONFIRMATION_TIMEOUT
    ) = ConsumeConnectionManager(
        "test-consume-connection",
        getRabbitMQConfiguration(rabbitMQContainer),
        getConnectionManagerConfiguration(prefetchCount, confirmationTimeout),
        sharedExecutor,
        channelChecker
    )

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}
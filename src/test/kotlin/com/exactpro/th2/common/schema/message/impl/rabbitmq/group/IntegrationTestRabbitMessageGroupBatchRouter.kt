/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.impl.context.DefaultMessageRouterContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import mu.KotlinLogging
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue

@IntegrationTest
class IntegrationTestRabbitMessageGroupBatchRouter {

    @Test
    fun `subscribe to exclusive queue`() {
        RabbitMQContainer(DockerImageName.parse("rabbitmq:3.8-management-alpine"))
            .use { rabbitMQContainer ->
                rabbitMQContainer.start()
                LOGGER.info { "Started with port ${rabbitMQContainer.amqpPort}" }

                createConnectionManager(rabbitMQContainer).use { firstManager ->
                    createRouter(firstManager).use { firstRouter ->
                        createConnectionManager(rabbitMQContainer).use { secondManager ->
                            createRouter(secondManager).use { secondRouter ->
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

    private fun createRouter(connectionManager: ConnectionManager) = RabbitMessageGroupBatchRouter()
        .apply {
            init(
                DefaultMessageRouterContext(
                    connectionManager,
                    mock { },
                    MessageRouterConfiguration()
                )
            )
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
        LOGGER.error { "Fatal connection problem" }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private const val DEFAULT_PREFETCH_COUNT = 10
        private val DEFAULT_CONFIRMATION_TIMEOUT: Duration = Duration.ofSeconds(1)
    }
}
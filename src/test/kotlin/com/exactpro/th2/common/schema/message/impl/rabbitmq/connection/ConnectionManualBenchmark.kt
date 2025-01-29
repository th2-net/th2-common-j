/*
 * Copyright 2024-2025 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.message.ContainerConstants.RABBITMQ_IMAGE_NAME
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.util.getRabbitMQConfiguration
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import org.testcontainers.containers.RabbitMQContainer
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom

object ConnectionManualBenchmark {
    private val LOGGER = KotlinLogging.logger {}
    private val channelChecker = Executors.newSingleThreadScheduledExecutor(ThreadFactoryBuilder().setNameFormat("channel-checker-%d").build())
    private val sharedExecutor = Executors.newFixedThreadPool(1, ThreadFactoryBuilder().setNameFormat("rabbitmq-shared-pool-%d").build())

    @JvmStatic
    fun main(args: Array<String>) {
        RabbitMQContainer(RABBITMQ_IMAGE_NAME).use { container ->
            val payload: ByteArray = ByteArray(256 * 1024).also {
                ThreadLocalRandom.current().nextBytes(it)
            }
            container.start()
            with(container) {
                val queueName = "test-queue"
                val exchangeName = "test-exchange"
                execInContainer("rabbitmqadmin", "declare", "queue", "name=$queueName", "durable=false")
                execInContainer("rabbitmqadmin", "declare", "exchange", "name=$exchangeName", "type=fanout")
                execInContainer("rabbitmqadmin", "declare", "binding", "source=$exchangeName", "destination_type=queue", "destination=$queueName")

                val consumer = createConsumeConnectionManager(
                    container,
                    ConnectionManagerConfiguration(
                        prefetchCount = 1000,
                        confirmationTimeout = Duration.ofSeconds(5),
                        enablePublisherConfirmation = true,
                        maxInflightPublicationsBytes = 1024 * 1024 * 25,
                    )
                )

                val connectionManagerWithConfirmation = {
                    createPublishConnectionManager(
                        container,
                        ConnectionManagerConfiguration(
                            prefetchCount = 100,
                            confirmationTimeout = Duration.ofSeconds(5),
                            enablePublisherConfirmation = true,
                            maxInflightPublicationsBytes = 1024 * 1024 * 1,
                        )
                    )
                }

                val connectionManagerWithoutConfirmation = {
                    createPublishConnectionManager(
                        container,
                        ConnectionManagerConfiguration(
                            prefetchCount = 100,
                            confirmationTimeout = Duration.ofSeconds(5),
                            enablePublisherConfirmation = false,
                            maxInflightPublicationsBytes = -1,
                        )
                    )
                }

                consumer.use {
                    consumer.basicConsume(
                        queueName,
                        { _, _, ack -> ack.confirm()  },
                        { LOGGER.warn { "Canceled" } },
                    )
                    val times = 5
                    val withConf = ArrayList<Duration>(times)
                    val withoutConf = ArrayList<Duration>(times)
                    repeat(times) {
                    // Avg for no confirmation: PT19.468S
                        withoutConf += measure("not confirmation", connectionManagerWithoutConfirmation, payload)

                    // Avg for confirmation: PT20.758S
                        withConf += measure("confirmation", connectionManagerWithConfirmation, payload)
                    }

                    fun List<Duration>.avg(): Duration {
                        return map { it.toMillis() }.average().let { Duration.ofMillis(it.toLong()) }
                    }

                    LOGGER.info { "Avg for confirmation: ${withConf.avg()}" }
                    LOGGER.info { "Avg for no confirmation: ${withoutConf.avg()}" }
                }
            }
        }
    }

    private fun measure(name: String, manager: () -> PublishConnectionManager, payload: ByteArray): Duration {
        LOGGER.info("Measuring $name")
        val start: Long
        val sent: Long
        manager().use { mgr ->
            repeat(100) {
                mgr.basicPublish(
                    "test-exchange",
                    "routing",
                    null,
                    payload,
                )
            }
            LOGGER.info("Wait after warmup for $name")
            Thread.sleep(1000)
            LOGGER.info("Start measuring for $name")
            start = System.currentTimeMillis()
            repeat(100_000) {
                mgr.basicPublish(
                    "test-exchange",
                    "routing",
                    null,
                    payload,
                )
            }
            sent = System.currentTimeMillis()
        }
        val end = System.currentTimeMillis()
        LOGGER.info("Sent $name in ${Duration.ofMillis(sent - start)}")
        val duration = Duration.ofMillis(end - start)
        LOGGER.info("Executed $name in $duration")
        return duration
    }

    private fun createPublishConnectionManager(container: RabbitMQContainer, configuration: ConnectionManagerConfiguration) =
        PublishConnectionManager(
            "test-publish-connection",
            getRabbitMQConfiguration(container),
            configuration,
            sharedExecutor,
            channelChecker
        )

    private fun createConsumeConnectionManager(container: RabbitMQContainer, configuration: ConnectionManagerConfiguration) =
        ConsumeConnectionManager(
            "test-consume-connection",
            getRabbitMQConfiguration(container),
            configuration,
            sharedExecutor,
            channelChecker
        )
}
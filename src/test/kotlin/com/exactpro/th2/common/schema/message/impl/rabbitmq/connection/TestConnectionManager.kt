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
import com.exactpro.th2.common.util.RabbitTestContainerUtil
import com.rabbitmq.client.AlreadyClosedException
import com.rabbitmq.client.BuiltinExchangeType
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.utility.DockerImageName

private val LOGGER = KotlinLogging.logger { }


@IntegrationTest
class TestConnectionManager {

    @Test
    fun `connection manager reports unacked messages when confirmation timeout elapsed`() {
        val routingKey = "routingKey"
        val queueName = "queue"
        val exchange = "test-exchange"
        val prefetchCount = 10
        RabbitMQContainer(DockerImageName.parse("rabbitmq:3.8-management-alpine"))
            .withExchange(exchange, BuiltinExchangeType.FANOUT.type, false, false, true, emptyMap())
            .withQueue(queueName)
            .withBinding(exchange, queueName, emptyMap(), routingKey, "queue")
            .use {
                it.start()
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val queue = ArrayBlockingQueue<ManualAckDeliveryCallback.Confirmation>(prefetchCount)
                val countDown = CountDownLatch(prefetchCount)
                val confirmationTimeout = Duration.ofSeconds(1)
                ConnectionManager(
                    RabbitMQConfiguration(
                        host = it.host,
                        vHost = "",
                        port = it.amqpPort,
                        username = it.adminUsername,
                        password = it.adminPassword,
                    ),
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = prefetchCount,
                        confirmationTimeout = confirmationTimeout,
                    ),
                ).use { manager ->
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

                    Assertions.assertTrue(
                        countDown.await(
                            1L,
                            TimeUnit.SECONDS
                        )
                    ) { "Not all messages were received: ${countDown.count}" }

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

    @Test
    fun `connection manager receives a message from a queue that did not exist at the time of subscription`() {
        val routingKey = "routingKey"
        val queueName = "queue"
        val exchange = "test-exchange"
        val prefetchCount = 10
        RabbitMQContainer(DockerImageName.parse("rabbitmq:3.8-management-alpine"))
            .withExchange(exchange, BuiltinExchangeType.FANOUT.type, false, false, true, emptyMap())
            .withQueue(queueName)
            .withBinding(exchange, queueName, emptyMap(), routingKey, "queue")
            .use {
                it.start()
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val counter = AtomicInteger(0)
                val confirmationTimeout = Duration.ofSeconds(1)
                ConnectionManager(
                    RabbitMQConfiguration(
                        host = it.host,
                        vHost = "",
                        port = it.amqpPort,
                        username = it.adminUsername,
                        password = it.adminPassword,
                    ),
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = prefetchCount,
                        confirmationTimeout = confirmationTimeout,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 200,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
                    Thread {
                        connectionManager.basicConsume("wrong-queue", { _, delivery, ack ->
                            LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                            counter.incrementAndGet()
                            ack.confirm()
                        }) {
                            LOGGER.info { "Canceled $it" }
                        }
                    }.start()

                    Thread.sleep(500)

                    LOGGER.info { "creating the queue..." }
                    RabbitTestContainerUtil.declareQueue(it, "wrong-queue")
                    LOGGER.info { RabbitTestContainerUtil.putMessageInQueue(it, "wrong-queue") }
                    LOGGER.info { "queues list: \n ${it.execInContainer("rabbitmqctl", "list_queues")}" }

                    Thread.sleep(500)

                    // todo check isReady and isAlive, it should be false at some point
                    Assertions.assertEquals(1, counter.get())
                    Assertions.assertTrue(connectionManager.isAlive)
                    Assertions.assertTrue(connectionManager.isReady)
                }
            }
    }

    @Test
    fun `connection manager sends a message to wrong exchange`() {
        val queueName = "queue"
        val exchange = "test-exchange"
        val prefetchCount = 10
        RabbitMQContainer(DockerImageName.parse("rabbitmq:3.8-management-alpine"))
            .withQueue(queueName)
            .use {
                it.start()
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val confirmationTimeout = Duration.ofSeconds(1)
                val counter = AtomicInteger(0)
                ConnectionManager(
                    RabbitMQConfiguration(
                        host = it.host,
                        vHost = "",
                        port = it.amqpPort,
                        username = it.adminUsername,
                        password = it.adminPassword,
                    ),
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = prefetchCount,
                        confirmationTimeout = confirmationTimeout,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 200,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
                    Thread {
                        connectionManager.basicConsume(queueName, { _, delivery, ack ->
                            counter.incrementAndGet()
                            LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                        }) {
                            LOGGER.info { "Canceled $it" }
                        }
                    }.start()

                    Thread.sleep(5000)

                    LOGGER.info { "Starting publishing..." }
                    connectionManager.basicPublish(exchange, "", null, "Hello1".toByteArray(Charsets.UTF_8))
                    Thread.sleep(1000)
                    LOGGER.info { "Publication finished!" }
                    RabbitTestContainerUtil.declareFanoutExchangeWithBinding(it, exchange, queueName)
                    Thread.sleep(1000)
                    Assertions.assertThrows(AlreadyClosedException::class.java) {
                        //todo there should be retry
                        connectionManager.basicPublish(exchange, "", null, "Hello2".toByteArray(Charsets.UTF_8))
                    }
                    Assertions.assertEquals(0, counter.get())

                }
            }
    }
}

//    @Test
//    @Disabled
//    fun `connection manager receives a messages after container restart`() {
//        val routingKey = "routingKey"
//        val queueName = "queue"
//        val exchange = "test-exchange"
//        val prefetchCount = 10
//        RabbitMQContainer(DockerImageName.parse("rabbitmq:3.8-management-alpine"))
////            .withExchange(exchange, BuiltinExchangeType.FANOUT.type, false, false, true, emptyMap())
//            .withQueue(queueName)
////            .withBinding(exchange, queueName, emptyMap(), routingKey, "queue")
//            .use {
//                it.start()
//                LOGGER.info { "Started with port ${it.amqpPort}" }
//                val counter = AtomicInteger(0)
//                val confirmationTimeout = Duration.ofSeconds(1)
//                ConnectionManager(
//                    RabbitMQConfiguration(
//                        host = it.host,
//                        vHost = "",
//                        port = it.amqpPort,
//                        username = it.adminUsername,
//                        password = it.adminPassword,
//                    ),
//                    ConnectionManagerConfiguration(
//                        subscriberName = "test",
//                        prefetchCount = prefetchCount,
//                        confirmationTimeout = confirmationTimeout,
//                        minConnectionRecoveryTimeout = 100,
//                        maxConnectionRecoveryTimeout = 200,
//                        maxRecoveryAttempts = 5
//                    ),
//                ).use { connectionManager ->
//                    Thread {
//                        connectionManager.basicConsume(queueName, { _, delivery, ack ->
//                            LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
//                            counter.incrementAndGet()
//                        }) {
//                            LOGGER.info { "Canceled $it" }
//                        }
//                    }.start()
//
//                    LOGGER.info { it.host + " " + it.httpPort + " " + it.amqpPort }
//
//
//                    LOGGER.info { it.host + " " + it.httpPort + " " + it.amqpPort }
//                    Thread.sleep(10000)
//
//                    LOGGER.info { "Starting publishing..." }
//                    RabbitTestContainerUtil.putMessageInQueue(it, queueName)
//                    LOGGER.info { "Publication finished!" }
//
//                    Assertions.assertEquals(1, counter.get())
//                    Thread.sleep(1000)
//
//                }
//            }
//    }
//}
//



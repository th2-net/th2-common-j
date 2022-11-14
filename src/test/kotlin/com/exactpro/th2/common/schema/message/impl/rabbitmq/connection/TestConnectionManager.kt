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
import com.exactpro.th2.common.util.RabbitTestContainerUtil.Companion.declareFanoutExchangeWithBinding
import com.exactpro.th2.common.util.RabbitTestContainerUtil.Companion.declareQueue
import com.exactpro.th2.common.util.RabbitTestContainerUtil.Companion.getQueuesInfo
import com.exactpro.th2.common.util.RabbitTestContainerUtil.Companion.putMessageInQueue
import com.exactpro.th2.common.util.RabbitTestContainerUtil.Companion.restartContainer
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile

private val LOGGER = KotlinLogging.logger { }


@IntegrationTest
class TestConnectionManager {

    @Test
    fun `connection manager reports unacked messages when confirmation timeout elapsed`() {
        val routingKey = "routingKey1"
        val queueName = "queue1"
        val exchange = "test-exchange1"
        val prefetchCount = 10
        rabbit
            .let {
                declareQueue(rabbit, queueName)
                declareFanoutExchangeWithBinding(rabbit, exchange, queueName)
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
        val queueName = "queue2"
        val exchange = "test-exchange2"
        val wrongQueue = "wrong-queue2"
        val prefetchCount = 10
        rabbit
            .let {
                declareQueue(rabbit, queueName)
                declareFanoutExchangeWithBinding(rabbit, exchange, queueName)
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
                        connectionManager.basicConsume(wrongQueue, { _, delivery, ack ->
                            LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                            counter.incrementAndGet()
                            ack.confirm()
                        }) {
                            LOGGER.info { "Canceled $it" }
                        }
                    }.start()

                    LOGGER.info { "creating the queue..." }
                    declareQueue(it, wrongQueue)
                    LOGGER.info {
                        "Adding message to the queue: \n" + putMessageInQueue(
                            it,
                            wrongQueue
                        )
                    }
                    LOGGER.info { "queues list: \n ${it.execInContainer("rabbitmqctl", "list_queues")}" }

                    // todo check isReady and isAlive, it should be false at some point
                    Assertions.assertEquals(
                        1,
                        counter.get()
                    ) { "Unexpected number of messages received. The message should be received" }
                    Assertions.assertTrue(connectionManager.isAlive)
                    Assertions.assertTrue(connectionManager.isReady)
                }
            }
    }

    @Test
    fun `connection manager sends a message to wrong exchange`() {
        val queueName = "queue3"
        val exchange = "test-exchange3"
        val prefetchCount = 10
        rabbit
            .let {
                declareQueue(rabbit, queueName)
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
                        connectionManager.basicConsume(queueName, { _, delivery, _ ->
                            counter.incrementAndGet()
                            LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from \"${delivery.envelope.routingKey}\"" }
                        }) {
                            LOGGER.info { "Canceled $it" }
                        }
                    }.start()

                    LOGGER.info { "Starting first publishing..." }
                    connectionManager.basicPublish(exchange, "", null, "Hello1".toByteArray(Charsets.UTF_8))
                    Thread.sleep(200)
                    LOGGER.info { "Publication finished!" }
                    Assertions.assertEquals(
                        0,
                        counter.get()
                    ) { "Unexpected number of messages received. The first message shouldn't be received" }
                    Thread.sleep(200)
                    LOGGER.info { "Creating the correct exchange..." }
                    declareFanoutExchangeWithBinding(it, exchange, queueName)
                    Thread.sleep(200)
                    LOGGER.info { "Exchange created!" }

                    Assertions.assertDoesNotThrow {
                        connectionManager.basicPublish(exchange, "", null, "Hello2".toByteArray(Charsets.UTF_8))
                    }

                    Thread.sleep(200)
                    Assertions.assertEquals(
                        1,
                        counter.get()
                    ) { "Unexpected number of messages received. The second message should be received" }

                }
            }
    }

    @Test
    fun `connection manager handles ack timeout`() {
        val configFilename = "rabbitmq_it.conf"
        val queueName = "queue4"
        val prefetchCount = 10

        RabbitMQContainer(DockerImageName.parse(RABBIT_IMAGE_NAME))
            .withRabbitMQConfig(MountableFile.forClasspathResource(configFilename))
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
                            LOGGER.info { "Received 1 ${delivery.body.toString(Charsets.UTF_8)} from \"${delivery.envelope.routingKey}\"" }
                            if (counter.get() != 0) {
                                ack.confirm()
                                LOGGER.info { "Confirmed!" }
                            } else {
                                LOGGER.info { "Left this message unacked" }
                            }
                            counter.incrementAndGet()
                        }) {
                            LOGGER.info { "Canceled $it" }
                        }
                    }.start()

                    LOGGER.info { "Sending first message" }
                    putMessageInQueue(it, queueName)

                    LOGGER.info { "queues list: \n ${getQueuesInfo(it)}" }
                    LOGGER.info { "Sleeping..." }
                    Thread.sleep(63000)

                    LOGGER.info { "Sending second message" }
                    putMessageInQueue(it, queueName)

                    val queuesListExecResult = getQueuesInfo(it)
                    LOGGER.info { "queues list: \n $queuesListExecResult" }

                    Assertions.assertEquals(3, counter.get()) { "Wrong number of received messages" }
                    Assertions.assertTrue(
                        queuesListExecResult.toString().contains("$queueName\t0")
                    ) { "There should be no messages left in the queue" }

                }
            }
    }

    @Test
    fun `connection manager receives a messages after container restart`() {
        val queueName = "queue5"
        val prefetchCount = 10
        val amqpPort = 5672
        val container = object : RabbitMQContainer(DockerImageName.parse(RABBIT_IMAGE_NAME)) {
            fun addFixedPort(hostPort: Int, containerPort: Int) {
                super.addFixedExposedPort(hostPort, containerPort)
            }
        }

        container
            .addFixedPort(amqpPort, amqpPort)
        container
            .withQueue(queueName)
            .use {
                it.start()
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val counter = AtomicInteger(0)
                val confirmationTimeout = Duration.ofSeconds(1)
                ConnectionManager(
                    RabbitMQConfiguration(
                        host = it.host,
                        vHost = "",
                        port = amqpPort,
                        username = it.adminUsername,
                        password = it.adminPassword,
                    ),
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = prefetchCount,
                        confirmationTimeout = confirmationTimeout,
                        minConnectionRecoveryTimeout = 10000,
                        maxConnectionRecoveryTimeout = 20000,
                        connectionTimeout = 10000,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->

                    Thread {
                        connectionManager.basicConsume(queueName, { _, delivery, ack ->
                            LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                            counter.incrementAndGet()
                            ack.confirm()
                        }) {
                            LOGGER.info { "Canceled $it" }
                        }
                    }.start()
                    LOGGER.info { "Rabbit address- ${it.host}:${it.amqpPort}" }

                    LOGGER.info { "Restarting the container" }
                    restartContainer(it)
                    Thread.sleep(5000)

                    LOGGER.info { "Rabbit address after restart - ${it.host}:${it.amqpPort}" }
                    LOGGER.info { getQueuesInfo(it) }

                    LOGGER.info { "Starting publishing..." }
                    putMessageInQueue(it, queueName)
                    LOGGER.info { "Publication finished!" }
                    LOGGER.info { getQueuesInfo(it) }

                    Assertions.assertEquals(1, counter.get()) { "Wrong number of received messages" }
                    Assertions.assertTrue(
                        getQueuesInfo(it).toString().contains("$queueName\t0")
                    ) { "There should be no messages left in the queue" }

                }
            }
    }

    @Test
    fun `connection manager publish a message and receives it`() {
        val queueName = "queue6"
        val prefetchCount = 10
        val exchange = "test-exchange6"
        val routingKey = "routingKey6"

        rabbit
            .let {
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
                        minConnectionRecoveryTimeout = 10000,
                        maxConnectionRecoveryTimeout = 20000,
                        connectionTimeout = 10000,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->

                    declareQueue(it, queueName)
                    declareFanoutExchangeWithBinding(it, exchange, queueName)

                    connectionManager.basicPublish(exchange, routingKey, null, "Hello1".toByteArray(Charsets.UTF_8))

                    Thread.sleep(500)
                    Thread {
                        connectionManager.basicConsume(queueName, { _, delivery, ack ->
                            LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                            counter.incrementAndGet()
                            ack.confirm()
                        }) {
                            LOGGER.info { "Canceled $it" }
                        }
                    }.start()
                    Thread.sleep(500)

                    Assertions.assertEquals(1, counter.get()) { "Wrong number of received messages" }
                    Assertions.assertTrue(
                        getQueuesInfo(it).toString().contains("$queueName\t0")
                    ) { "There should be no messages left in the queue" }

                }
            }
    }

    companion object {

        private const val RABBIT_IMAGE_NAME = "rabbitmq:3.8-management-alpine"
        private lateinit var rabbit: RabbitMQContainer

        @BeforeAll
        @JvmStatic
        fun initRabbit() {
            rabbit =
                RabbitMQContainer(DockerImageName.parse(RABBIT_IMAGE_NAME))
            rabbit.start()
        }

        @AfterAll
        @JvmStatic
        fun closeRabbit() {
            rabbit.close()
        }
    }


}




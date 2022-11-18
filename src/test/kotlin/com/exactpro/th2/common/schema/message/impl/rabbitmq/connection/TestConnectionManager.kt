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
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.exactpro.th2.common.util.RabbitTestContainerUtil.Companion.declareFanoutExchangeWithBinding
import com.exactpro.th2.common.util.RabbitTestContainerUtil.Companion.declareQueue
import com.exactpro.th2.common.util.RabbitTestContainerUtil.Companion.getQueuesInfo
import com.exactpro.th2.common.util.RabbitTestContainerUtil.Companion.putMessageInQueue
import com.exactpro.th2.common.util.RabbitTestContainerUtil.Companion.restartContainer
import com.rabbitmq.client.BuiltinExchangeType
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
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
        rabbit
            .let {
                declareQueue(rabbit, queueName)
                declareFanoutExchangeWithBinding(rabbit, exchange, queueName)
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val queue = ArrayBlockingQueue<ManualAckDeliveryCallback.Confirmation>(PREFETCH_COUNT)
                val countDown = CountDownLatch(PREFETCH_COUNT)
                createConnectionManager(
                    it, ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = PREFETCH_COUNT,
                        confirmationTimeout = CONFIRMATION_TIMEOUT,
                    )
                ).use { manager ->
                    manager.basicConsume(queueName, { _, delivery, ack ->
                        LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                        queue += ack
                        countDown.countDown()
                    }) {
                        LOGGER.info { "Canceled $it" }
                    }

                    repeat(PREFETCH_COUNT + 1) { index ->
                        manager.basicPublish(exchange, routingKey, null, "Hello $index".toByteArray(Charsets.UTF_8))
                    }

                    countDown.assertComplete("Not all messages were received")

                    assertTrue(manager.isAlive) { "Manager should still be alive" }
                    assertTrue(manager.isReady) { "Manager should be ready until the confirmation timeout expires" }

                    Thread.sleep(CONFIRMATION_TIMEOUT.toMillis() + 100/*just in case*/) // wait for confirmation timeout

                    assertTrue(manager.isAlive) { "Manager should still be alive" }
                    assertFalse(manager.isReady) { "Manager should not be ready" }

                    queue.poll().confirm()

                    assertTrue(manager.isAlive) { "Manager should still be alive" }
                    assertTrue(manager.isReady) { "Manager should be ready" }

                    val receivedData = generateSequence { queue.poll(10L, TimeUnit.MILLISECONDS) }
                        .onEach(ManualAckDeliveryCallback.Confirmation::confirm)
                        .count()
                    assertEquals(PREFETCH_COUNT, receivedData) { "Unexpected number of messages received" }
                }
            }
    }

    @Test
    fun `connection manager receives a message from a queue that did not exist at the time of subscription`() {
        val wrongQueue = "wrong-queue2"
        rabbit
            .let { rabbitMQContainer ->
                LOGGER.info { "Started with port ${rabbitMQContainer.amqpPort}" }
                createConnectionManager(
                    rabbitMQContainer,
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = PREFETCH_COUNT,
                        confirmationTimeout = CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 200,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
                    var thread: Thread? = null
                    var monitor: SubscriberMonitor? = null
                    val consume = CountDownLatch(1)
                    try {
                        thread = thread {
                            monitor = connectionManager.basicConsume(wrongQueue, { _, delivery, ack ->
                                LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                                consume.countDown()
                                ack.confirm()
                            }) {
                                LOGGER.info { "Canceled $it" }
                            }
                        }

                        assertTarget(true, "Thread for consuming isn't started", thread::isAlive)
                        // todo check isReady and isAlive, it should be false at some point
//                        assertTarget(false, "Readiness probe doesn't fall down", connectionManager::isReady)

                        LOGGER.info { "creating the queue..." }
                        declareQueue(rabbitMQContainer, wrongQueue)
                        assertTarget(false, "Thread for consuming isn't completed", thread::isAlive)

                        LOGGER.info {
                            "Adding message to the queue:\n${putMessageInQueue(rabbitMQContainer, wrongQueue)}"
                        }
                        LOGGER.info { "queues list: \n ${rabbitMQContainer.execInContainer("rabbitmqctl", "list_queues")}" }

                        consume.assertComplete("Unexpected number of messages received. The message should be received")

                        assertTrue(connectionManager.isAlive)
                        assertTrue(connectionManager.isReady)
                    } finally {
                        Assertions.assertDoesNotThrow {
                            monitor?.unsubscribe()
                        }
                        thread?.let {
                            thread.interrupt()
                            thread.join(100)
                            assertFalse(thread.isAlive)
                        }
                    }

                }
            }
    }

    @Test
    fun `connection manager sends a message to wrong exchange`() {
        val queueName = "queue3"
        val exchange = "test-exchange3"
        rabbit
            .let {
                declareQueue(rabbit, queueName)
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val counter = AtomicInteger(0)
                createConnectionManager(
                    it,
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = PREFETCH_COUNT,
                        confirmationTimeout = CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 200,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
                    var monitor: SubscriberMonitor? = null
                    try {
                        monitor = connectionManager.basicConsume(queueName, { _, delivery, _ ->
                            counter.incrementAndGet()
                            LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from \"${delivery.envelope.routingKey}\"" }
                        }) {
                            LOGGER.info { "Canceled $it" }
                        }

                        LOGGER.info { "Starting first publishing..." }
                        connectionManager.basicPublish(exchange, "", null, "Hello1".toByteArray(Charsets.UTF_8))
                        Thread.sleep(200)
                        LOGGER.info { "Publication finished!" }
                        assertEquals(
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
                        assertEquals(
                            1,
                            counter.get()
                        ) { "Unexpected number of messages received. The second message should be received" }
                    } finally {
                        Assertions.assertNotNull(monitor)
                        Assertions.assertDoesNotThrow {
                            monitor!!.unsubscribe()
                        }
                    }

                }
            }
    }

    @Test
    fun `connection manager handles ack timeout`() {
        val configFilename = "rabbitmq_it.conf"
        val queueName = "queue4"

        RabbitMQContainer(DockerImageName.parse(RABBIT_IMAGE_NAME))
            .withRabbitMQConfig(MountableFile.forClasspathResource(configFilename))
            .withQueue(queueName)
            .use {
                it.start()
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val counter = AtomicInteger(0)
                createConnectionManager(
                    it,
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = PREFETCH_COUNT,
                        confirmationTimeout = CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 200,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
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

                    LOGGER.info { "Sending first message" }
                    putMessageInQueue(it, queueName)

                    LOGGER.info { "queues list: \n ${getQueuesInfo(it)}" }
                    LOGGER.info { "Sleeping..." }
                    Thread.sleep(63000)

                    LOGGER.info { "Sending second message" }
                    putMessageInQueue(it, queueName)

                    val queuesListExecResult = getQueuesInfo(it)
                    LOGGER.info { "queues list: \n $queuesListExecResult" }

                    assertEquals(3, counter.get()) { "Wrong number of received messages" }
                    assertTrue(
                        queuesListExecResult.toString().contains("$queueName\t0")
                    ) { "There should be no messages left in the queue" }

                }
            }
    }

    @Test
    fun `connection manager handles ack timeout with several channels`() {
        val configFilename = "rabbitmq_it.conf"
        val queueNames = arrayOf("separate_queues1", "separate_queues2", "separate_queues3")

        RabbitMQContainer(DockerImageName.parse(RABBIT_IMAGE_NAME))
            .withRabbitMQConfig(MountableFile.forClasspathResource(configFilename))
            .withQueue(queueNames[0])
            .withQueue(queueNames[1])
            .withQueue(queueNames[2])
            .use {
                it.start()
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val counters = mapOf(
                    queueNames[0] to AtomicInteger(),           // this subscriber won't ack the first delivery
                    queueNames[1] to AtomicInteger(-1), // this subscriber won't ack two first deliveries
                    queueNames[2] to AtomicInteger(1)
                )
                createConnectionManager(
                    it,
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = PREFETCH_COUNT,
                        confirmationTimeout = CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 200,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->

                    fun subscribeOnQueue(
                        queue: String
                    ) {
                        connectionManager.basicConsume(queue, { _, delivery, ack ->
                            LOGGER.info { "Received from queue $queue ${delivery.body.toString(Charsets.UTF_8)}" }
                            if (counters[queue]!!.get() > 0) {
                                ack.confirm()
                                LOGGER.info { "Confirmed message form $queue" }
                            } else {
                                LOGGER.info { "Left this message from $queue unacked" }
                            }
                            counters[queue]!!.incrementAndGet()
                        }, {
                            LOGGER.info { "Canceled message form queue $queue" }
                        })
                    }

                    subscribeOnQueue(queueNames[0])
                    subscribeOnQueue(queueNames[1])
                    subscribeOnQueue(queueNames[2])

                    LOGGER.info { "Sending the first message batch" }
                    putMessageInQueue(it, queueNames[0])
                    putMessageInQueue(it, queueNames[1])
                    putMessageInQueue(it, queueNames[2])

                    LOGGER.info { "queues list: \n ${getQueuesInfo(it)}" }
                    LOGGER.info { "Sleeping..." }
                    Thread.sleep(30000)

                    LOGGER.info { "Sending the second message batch" }
                    putMessageInQueue(it, queueNames[0])
                    putMessageInQueue(it, queueNames[1])
                    putMessageInQueue(it, queueNames[2])

                    LOGGER.info { "Still sleeping. Waiting for PRECONDITION_FAILED..." }
                    Thread.sleep(32000)

                    LOGGER.info { "Sending the third message batch" }
                    putMessageInQueue(it, queueNames[0])
                    putMessageInQueue(it, queueNames[1])
                    putMessageInQueue(it, queueNames[2])

                    val queuesListExecResult = getQueuesInfo(it)
                    LOGGER.info { "queues list: \n $queuesListExecResult" }

                    for (queueName in queueNames) {
                        assertTrue(queuesListExecResult.toString().contains("$queueName\t0"))
                        { "There should be no messages left in queue $queueName" }
                    }


                    // 0 + 1 failed ack + 2 successful ack + 1 ack of requeued message
                    assertEquals(4, counters[queueNames[0]]!!.get())
                    { "Wrong number of received messages from queue ${queueNames[0]}" }
                    // -1 + 2 failed ack + 2 ack of requeued message + 1 successful ack
                    assertEquals(4, counters[queueNames[1]]!!.get())
                    { "Wrong number of received messages from queue ${queueNames[1]}" }
                    assertEquals(4, counters[queueNames[2]]!!.get())
                    { "Wrong number of received messages from queue ${queueNames[2]}" }

                }
            }
    }

    @Test
    fun `connection manager receives a messages after container restart`() {
        val queueName = "queue5"
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
                        prefetchCount = PREFETCH_COUNT,
                        confirmationTimeout = CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 1000,
                        maxConnectionRecoveryTimeout = 2000,
                        connectionTimeout = 1000,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
                    val consume = CountDownLatch(1)
                    connectionManager.basicConsume(queueName, { _, delivery, ack ->
                        LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                        consume.countDown()
                        ack.confirm()
                    }) {
                        LOGGER.info { "Canceled $it" }
                    }
                    LOGGER.info { "Rabbit address- ${it.host}:${it.amqpPort}" }

                    LOGGER.info { "Restarting the container" }
                    restartContainer(it)

                    LOGGER.info { "Rabbit address after restart - ${it.host}:${it.amqpPort}" }
                    LOGGER.info { getQueuesInfo(it) }

                    LOGGER.info { "Starting publishing..." }
                    putMessageInQueue(it, queueName)
                    LOGGER.info { "Publication finished!" }
                    LOGGER.info { getQueuesInfo(it) }

                    consume.assertComplete("Wrong number of received messages")
                    assertTrue(
                        getQueuesInfo(it).toString().contains("$queueName\t0")
                    ) { "There should be no messages left in the queue" }

                }
            }
    }

    @Test
    fun `connection manager publish a message and receives it`() {
        val queueName = "queue6"
        val exchange = "test-exchange6"
        val routingKey = "routingKey6"

        rabbit
            .let {
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val counter = AtomicInteger(0)
                createConnectionManager(
                    it,
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = PREFETCH_COUNT,
                        confirmationTimeout = CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 10000,
                        maxConnectionRecoveryTimeout = 20000,
                        connectionTimeout = 10000,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
                    var monitor: SubscriberMonitor? = null
                    try {
                        declareQueue(it, queueName)
                        declareFanoutExchangeWithBinding(it, exchange, queueName)

                        connectionManager.basicPublish(exchange, routingKey, null, "Hello1".toByteArray(Charsets.UTF_8))

                        Thread.sleep(200)
                    monitor =connectionManager.basicConsume(queueName, { _, delivery, ack ->
                        LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                        counter.incrementAndGet()
                        ack.confirm()
                    }) {
                        LOGGER.info { "Canceled $it" }
                    }
                    Thread.sleep(200)

                        assertEquals(1, counter.get()) { "Wrong number of received messages" }
                        assertTrue(
                            getQueuesInfo(it).toString().contains("$queueName\t0")
                        ) { "There should be no messages left in the queue" }
                    } finally {
                        Assertions.assertNotNull(monitor)
                        Assertions.assertDoesNotThrow {
                            monitor!!.unsubscribe()
                        }
                    }

                }
            }
    }

    @Test
    fun `connection manager handles ack timeout on queue with publishing by the manager`() {
        val configFilename = "rabbitmq_it.conf"
        val queueName = "queue7"
        val exchange = "test-exchange7"
        val routingKey = "routingKey7"


        RabbitMQContainer(DockerImageName.parse(RABBIT_IMAGE_NAME))
            .withRabbitMQConfig(MountableFile.forClasspathResource(configFilename))
            .withExchange(exchange, BuiltinExchangeType.FANOUT.type, false, false, true, emptyMap())
            .withQueue(queueName)
            .withBinding(exchange, queueName, emptyMap(), routingKey, "queue")
            .use {
                it.start()
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val counter = AtomicInteger(0)
                createConnectionManager(
                    it,
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = PREFETCH_COUNT,
                        confirmationTimeout = CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 200,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
                    connectionManager.basicConsume(queueName, { _, delivery, ack ->
                        LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} " }
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


                    LOGGER.info { "Sending the first message" }
                    connectionManager.basicPublish(exchange, routingKey, null, "Hello1".toByteArray(Charsets.UTF_8))

                    LOGGER.info { "queues list: \n ${getQueuesInfo(it)}" }
                    LOGGER.info { "Sleeping..." }
                    Thread.sleep(33000)


                    LOGGER.info { "Sending the second message" }
                    connectionManager.basicPublish(exchange, routingKey, null, "Hello2".toByteArray(Charsets.UTF_8))

                    Thread.sleep(30000)

                    LOGGER.info { "Sending the third message" }
                    connectionManager.basicPublish(exchange, routingKey, null, "Hello3".toByteArray(Charsets.UTF_8))

                    val queuesListExecResult = getQueuesInfo(it)
                    LOGGER.info { "queues list: \n $queuesListExecResult" }

                    assertEquals(4, counter.get()) { "Wrong number of received messages" }
                    assertTrue(
                        queuesListExecResult.toString().contains("$queueName\t0")
                    ) { "There should be no messages left in the queue" }

                }
            }
    }

    @Test
    fun `thread interruption test`() {
        val queueName = "queue8"
        rabbit
            .let {
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val counter = AtomicInteger(0)
                createConnectionManager(
                    it,
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = PREFETCH_COUNT,
                        confirmationTimeout = CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 2000,
                        connectionTimeout = 1000,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
                    var monitor: SubscriberMonitor? = null
                    var thread: Thread? = null
                    try {
                        thread = thread {
                            // marker that thread is actually running
                            monitor = connectionManager.basicConsume(queueName, { _, delivery, ack ->
                                LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                                counter.incrementAndGet()
                                ack.confirm()
                            }) {
                                LOGGER.info { "Canceled $it" }
                            }
                        }
                        Thread.sleep(2000)
                        Assertions.assertTrue(thread.isAlive)
                        LOGGER.info { "Interrupting..." }
                        thread.interrupt()
                        LOGGER.info { "Interrupted!" }
                        Thread.sleep(1000)
                        LOGGER.info { "Sleep done" }

                        Assertions.assertFalse(thread.isAlive)

                        Assertions.assertEquals(0, counter.get()) { "Wrong number of received messages" }
                    } finally {
                        Assertions.assertDoesNotThrow {
                            monitor?.unsubscribe()
                        }
                        Assertions.assertNotNull(thread)
                        thread?.interrupt()
                        thread?.join(100)
                        Assertions.assertFalse(thread!!.isAlive)
                    }
                }
            }
    }

    @Test
    fun `connection manager handles subscription cancel`() {
        val queueName = "queue9"
        rabbit
            .let {
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val counter = AtomicInteger(0)
                createConnectionManager(
                    it,
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = PREFETCH_COUNT,
                        confirmationTimeout = CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 2000,
                        connectionTimeout = 1000,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->

                    var thread: Thread? = null
                    var monitor: SubscriberMonitor?
                    try {
                        declareQueue(it, queueName)

                        thread = thread {
                            monitor = connectionManager.basicConsume(queueName, { _, delivery, ack ->
                                LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                                counter.incrementAndGet()
                                ack.confirm()
                            }) {
                                LOGGER.info { "Canceled $it" }
                            }

                            Thread.sleep(3500)
                            LOGGER.info { "Unsubscribing..." }
                            monitor!!.unsubscribe()
                        }
                        for (i in 1..5) {
                            putMessageInQueue(it, queueName)
                            Thread.sleep(1000)
                        }

                        assertEquals(3, counter.get()) { "Wrong number of received messages" }
                        assertTrue(
                            getQueuesInfo(it).toString().contains("$queueName\t2")
                        ) { "There should be messages in the queue" }
                    } finally {
                        Assertions.assertNotNull(thread)
                        Assertions.assertDoesNotThrow {
                            thread!!.interrupt()
                        }
                        Assertions.assertFalse(thread!!.isAlive)
                    }

                }
            }
    }

    @Test
    fun `connection manager handles ack timeout and subscription cancel`() {
        val configFilename = "rabbitmq_it.conf"
        val queueName = "queue"

        RabbitMQContainer(DockerImageName.parse(RABBIT_IMAGE_NAME))
            .withRabbitMQConfig(MountableFile.forClasspathResource(configFilename))
            .withQueue(queueName)
            .use {
                it.start()
                LOGGER.info { "Started with port ${it.amqpPort}" }
                val counter = AtomicInteger(0)
                createConnectionManager(
                    it,
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = PREFETCH_COUNT,
                        confirmationTimeout = CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 200,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
                    var thread: Thread? = null
                    try {
                        thread = thread {
                            val subscriberMonitor = connectionManager.basicConsume(queueName, { _, delivery, ack ->
                                LOGGER.info { "Received 1 ${delivery.body.toString(Charsets.UTF_8)} from \"${delivery.envelope.routingKey}\"" }
                                if (counter.get() == 0) {
                                    ack.confirm()
                                    LOGGER.info { "Confirmed!" }
                                } else {
                                    LOGGER.info { "Left this message unacked" }
                                }
                                counter.incrementAndGet()
                            }) {
                                LOGGER.info { "Canceled $it" }
                            }

                            Thread.sleep(30000)
                            LOGGER.info { "Unsubscribing..." }
                            subscriberMonitor.unsubscribe()
                        }

                        LOGGER.info { "Sending first message" }
                        putMessageInQueue(it, queueName)

                        LOGGER.info { "queues list: \n ${getQueuesInfo(it)}" }

                        LOGGER.info { "Sending second message" }
                        putMessageInQueue(it, queueName)
                        LOGGER.info { "Sleeping..." }
                        Thread.sleep(63000)

                        LOGGER.info { "queues list: \n ${getQueuesInfo(it)}" }


                        val queuesListExecResult = getQueuesInfo(it)
                        LOGGER.info { "queues list: \n $queuesListExecResult" }

                        assertEquals(2, counter.get()) { "Wrong number of received messages" }
                        assertTrue(
                            queuesListExecResult.toString().contains("$queueName\t1")
                        ) { "There should a message left in the queue" }
                    } finally {
                        Assertions.assertNotNull(thread)
                        Assertions.assertDoesNotThrow {
                            thread!!.interrupt()
                        }
                        Assertions.assertFalse(thread!!.isAlive)
                    }
                }
            }
    }

    private fun CountDownLatch.assertComplete(message: String) {
        assertTrue(
            await(
                1L,
                TimeUnit.SECONDS
            )
        ) { "$message, actual count: $count" }
    }

    private fun assertTarget(target: Boolean, message: String, func: () -> Boolean) {
        val start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < 1_000) {
            if (func() == target) {
                return
            }
            Thread.yield()
        }
        assertEquals(target, func(), message)
    }
    private fun createConnectionManager(container: RabbitMQContainer, configuration: ConnectionManagerConfiguration) =
        ConnectionManager(
            RabbitMQConfiguration(
                host = container.host,
                vHost = "",
                port = container.amqpPort,
                username = container.adminUsername,
                password = container.adminPassword,
            ),
            configuration
        )

    companion object {

        private const val RABBIT_IMAGE_NAME = "rabbitmq:3.8-management-alpine"
        private lateinit var rabbit: RabbitMQContainer
        private const val PREFETCH_COUNT = 10
        private val CONFIRMATION_TIMEOUT = Duration.ofSeconds(1)

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




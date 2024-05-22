/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.schema.message.ContainerConstants.DEFAULT_CONFIRMATION_TIMEOUT
import com.exactpro.th2.common.schema.message.ContainerConstants.DEFAULT_PREFETCH_COUNT
import com.exactpro.th2.common.schema.message.ContainerConstants.RABBITMQ_IMAGE_NAME
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.exactpro.th2.common.util.declareFanoutExchangeWithBinding
import com.exactpro.th2.common.util.declareQueue
import com.exactpro.th2.common.util.getChannelsInfo
import com.exactpro.th2.common.util.getQueuesInfo
import com.exactpro.th2.common.util.getSubscribedChannelsCount
import com.exactpro.th2.common.util.putMessageInQueue
import com.github.dockerjava.api.model.Capability
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Delivery
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.MountableFile
import java.io.IOException
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.test.assertFailsWith

@IntegrationTest
class TestConnectionManager {

    @Test
    fun `connection manager redelivers unconfirmed messages`() {
        val routingKey = "routingKey1"
        val queueName = "queue1"
        val exchange = "test-exchange1"
        rabbit
            .let { rabbit ->
                declareQueue(rabbit, queueName)
                declareFanoutExchangeWithBinding(rabbit, exchange, queueName)
                LOGGER.info { "Started with port ${rabbit.amqpPort}" }
                val messagesCount = 10
                val countDown = CountDownLatch(messagesCount)
                createConnectionManager(
                    rabbit, ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = DEFAULT_PREFETCH_COUNT,
                        confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
                        enablePublisherConfirmation = true,
                        maxInflightPublications = 5,
                        heartbeatIntervalSeconds = 1,
                        minConnectionRecoveryTimeout = 2000,
                        maxConnectionRecoveryTimeout = 2000,
                        // to avoid unexpected delays before recovery
                        retryTimeDeviationPercent = 0,
                    )
                ).use { manager ->
                    val receivedMessages = linkedSetOf<String>()
                    manager.basicConsume(queueName, { _, delivery, ack ->
                        val message = delivery.body.toString(Charsets.UTF_8)
                        LOGGER.info { "Received $message from ${delivery.envelope.routingKey}" }
                        if (receivedMessages.add(message)) {
                            // decrement only unique messages
                            countDown.countDown()
                        } else {
                            LOGGER.warn { "Duplicated $message for ${delivery.envelope.routingKey}" }
                        }
                        ack.confirm()
                    }) {
                        LOGGER.info { "Canceled $it" }
                    }


                    repeat(messagesCount) { index ->
                        if (index == 1) {
                            // delay should allow ack for the first message be received
                            Thread.sleep(100)
                            // Man pages:
                            // https://man7.org/linux/man-pages/man8/tc-netem.8.html
                            // https://man7.org/linux/man-pages/man8/ifconfig.8.html
                            //
                            // Here we try to emulate network outage to cause missing publication confirmations.
                            //
                            // In real life we will probably get duplicates in this case because
                            // rabbitmq does not provide exactly-once semantic.
                            // So, we will have to deal with it on the consumer side
                            rabbit.executeInContainerWithLogging("ifconfig", "eth0", "down")
                        } else if (index == 4) {
                            // More than 2 HB will be missed
                            // This is enough for rabbitmq server to understand the connection is lost
                            Thread.sleep(4_000)
                            // enabling network interface back
                            rabbit.executeInContainerWithLogging("ifconfig", "eth0", "up")
                        }
                        manager.basicPublish(exchange, routingKey, null, "Hello $index".toByteArray(Charsets.UTF_8))
                    }

                    countDown.assertComplete("Not all messages were received: $receivedMessages")
                    assertEquals(
                        (0 until messagesCount).map {
                            "Hello $it"
                        },
                        receivedMessages.toList(),
                        "messages received in unexpected order",
                    )
                }
            }
    }

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
                val queue = ArrayBlockingQueue<ManualAckDeliveryCallback.Confirmation>(DEFAULT_PREFETCH_COUNT)
                val countDown = CountDownLatch(DEFAULT_PREFETCH_COUNT)
                createConnectionManager(
                    it, ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = DEFAULT_PREFETCH_COUNT,
                        confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
                    )
                ).use { manager ->
                    manager.basicConsume(queueName, { _, delivery, ack ->
                        LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                        queue += ack
                        countDown.countDown()
                    }) {
                        LOGGER.info { "Canceled $it" }
                    }

                    repeat(DEFAULT_PREFETCH_COUNT + 1) { index ->
                        manager.basicPublish(exchange, routingKey, null, "Hello $index".toByteArray(Charsets.UTF_8))
                    }

                    countDown.assertComplete("Not all messages were received")

                    assertTrue(manager.isAlive) { "Manager should still be alive" }
                    assertTrue(manager.isReady) { "Manager should be ready until the confirmation timeout expires" }

                    Thread.sleep(DEFAULT_CONFIRMATION_TIMEOUT.toMillis() + 100/*just in case*/) // wait for confirmation timeout

                    assertTrue(manager.isAlive) { "Manager should still be alive" }
                    assertFalse(manager.isReady) { "Manager should not be ready" }

                    queue.poll().confirm()

                    assertTrue(manager.isAlive) { "Manager should still be alive" }
                    assertTrue(manager.isReady) { "Manager should be ready" }

                    val receivedData = generateSequence { queue.poll(10L, TimeUnit.MILLISECONDS) }
                        .onEach(ManualAckDeliveryCallback.Confirmation::confirm)
                        .count()
                    assertEquals(DEFAULT_PREFETCH_COUNT, receivedData) { "Unexpected number of messages received" }
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
                        prefetchCount = DEFAULT_PREFETCH_COUNT,
                        confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
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

                        assertTarget(true, message = "Thread for consuming isn't started", func = thread::isAlive)
                        // todo check isReady and isAlive, it should be false at some point
//                        assertTarget(false, "Readiness probe doesn't fall down", connectionManager::isReady)

                        LOGGER.info { "creating the queue..." }
                        declareQueue(rabbitMQContainer, wrongQueue)
                        assertTarget(false, message = "Thread for consuming isn't completed", func = thread::isAlive)

                        LOGGER.info {
                            "Adding message to the queue:\n${putMessageInQueue(rabbitMQContainer, wrongQueue)}"
                        }
                        LOGGER.info {
                            "queues list: \n ${
                                rabbitMQContainer.execInContainer(
                                    "rabbitmqctl",
                                    "list_queues"
                                )
                            }"
                        }

                        consume.assertComplete("Unexpected number of messages received. The message should be received")

                        assertEquals(1, getSubscribedChannelsCount(rabbitMQContainer, wrongQueue))
                        assertTrue(connectionManager.isAlive)
                        assertTrue(connectionManager.isReady)
                    } finally {
                        Assertions.assertDoesNotThrow {
                            monitor!!.unsubscribe()
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
                        prefetchCount = DEFAULT_PREFETCH_COUNT,
                        confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
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

        RabbitMQContainer(RABBITMQ_IMAGE_NAME)
            .withRabbitMQConfig(MountableFile.forClasspathResource(configFilename))
            .withQueue(queueName)
            .use {
                it.start()
                LOGGER.info { "Started with port ${it.amqpPort}" }

                createConnectionManager(
                    it,
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = DEFAULT_PREFETCH_COUNT,
                        confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 200,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
                    val consume = CountDownLatch(3)

                    connectionManager.basicConsume(queueName, { _, delivery, _ ->
                        LOGGER.info { "Received 1 ${delivery.body.toString(Charsets.UTF_8)} from \"${delivery.envelope.routingKey}\"" }
                        consume.countDown()
                    }) {
                        LOGGER.info { "Canceled $it" }
                    }

                    LOGGER.info { "Sending first message" }
                    putMessageInQueue(it, queueName)
                    assertTarget(3 - 1, message = "Consume first message") { consume.count }

                    LOGGER.info { "queues list: \n ${getQueuesInfo(it)}" }
                    val channels1 = getChannelsInfo(it)

                    LOGGER.info { channels1 }
                    LOGGER.info { "Waiting for ack timeout ..." }

                    assertTarget(3 - 2, 63_000, "Consume first message again") { consume.count }
                    val channels2 = getChannelsInfo(it)
                    LOGGER.info { channels2 }

                    LOGGER.info { "Sending second message" }
                    putMessageInQueue(it, queueName)

                    val queuesListExecResult = getQueuesInfo(it)
                    LOGGER.info { "queues list: \n $queuesListExecResult" }

                    assertEquals(1, getSubscribedChannelsCount(it, queueName))
                    { "There is must be single channel after recovery" }
                    assertNotEquals(channels1, channels2) { "The recovered channel must have another pid" }

                    consume.assertComplete("Wrong number of received messages")
                    assertTrue(
                        queuesListExecResult.toString().contains("$queueName\t2")
                    ) { "There should be no messages left in the queue" }

                }
            }
    }

    @Test
    fun `connection manager handles ack timeout with several channels`() {
        val configFilename = "rabbitmq_it.conf"
        val queueNames = arrayOf("separate_queues1", "separate_queues2", "separate_queues3")

        RabbitMQContainer(RABBITMQ_IMAGE_NAME)
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
                        prefetchCount = DEFAULT_PREFETCH_COUNT,
                        confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
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
                        assertEquals(1, getSubscribedChannelsCount(it, queueName))
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
        val container = RabbitMQContainer(RABBITMQ_IMAGE_NAME)
            .apply {
                portBindings = listOf("$amqpPort:$amqpPort")
            }

        container
            .use {
                it.start()
                declareQueue(it, queueName)
                LOGGER.info { "Started with port ${it.amqpPort}" }
                ConnectionManager(
                    "test-connection",
                    RabbitMQConfiguration(
                        host = it.host,
                        vHost = "",
                        port = amqpPort,
                        username = it.adminUsername,
                        password = it.adminPassword,
                    ),
                    ConnectionManagerConfiguration(
                        subscriberName = "test",
                        prefetchCount = DEFAULT_PREFETCH_COUNT,
                        confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
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
                    it.stop()
                    it.start()
                    Thread.sleep(5_000)
                    declareQueue(it, queueName)
                    Thread.sleep(5_000)

                    LOGGER.info { "Rabbit address after restart - ${it.host}:${it.amqpPort}" }
                    LOGGER.info { getQueuesInfo(it) }

                    LOGGER.info { "Starting publishing..." }
                    putMessageInQueue(it, queueName)
                    assertEquals(1, getSubscribedChannelsCount(it, queueName))

                    LOGGER.info { "Publication finished!" }
                    LOGGER.info { getQueuesInfo(it) }

                    consume.assertComplete("Wrong number of received messages")
                    assertTrue(getQueuesInfo(it).toString().contains("$queueName\t0")) {
                        "There should be no messages left in the queue"
                    }
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
                        prefetchCount = DEFAULT_PREFETCH_COUNT,
                        confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
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
                        monitor = connectionManager.basicConsume(queueName, { _, delivery, ack ->
                            LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                            counter.incrementAndGet()
                            ack.confirm()
                        }) {
                            LOGGER.info { "Canceled $it" }
                        }
                        Thread.sleep(200)

                        assertEquals(1, getSubscribedChannelsCount(it, queueName))
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


        RabbitMQContainer(RABBITMQ_IMAGE_NAME)
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
                        prefetchCount = DEFAULT_PREFETCH_COUNT,
                        confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
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

                    assertEquals(1, getSubscribedChannelsCount(it, queueName))
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
                        prefetchCount = DEFAULT_PREFETCH_COUNT,
                        confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
                        minConnectionRecoveryTimeout = 100,
                        maxConnectionRecoveryTimeout = 2000,
                        connectionTimeout = 1000,
                        maxRecoveryAttempts = 5
                    ),
                ).use { connectionManager ->
                    var thread: Thread? = null
                    try {
                        thread = thread {
                            connectionManager.basicConsume(queueName, { _, delivery, ack ->
                                LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.routingKey}" }
                                counter.incrementAndGet()
                                ack.confirm()
                            }) {
                                LOGGER.info { "Canceled $it" }
                            }
                        }

                        assertTarget(true, message = "Thread for consuming isn't started", func = thread::isAlive)
                        Thread.sleep(1000)
                        assertTrue(thread.isAlive)
                        LOGGER.info { "Interrupting..." }
                        thread.interrupt()
                        LOGGER.info { "Interrupted!" }
                        assertTarget(false, message = "Thread for consuming isn't stopped", func = thread::isAlive)
                        assertEquals(0, counter.get()) { "Wrong number of received messages" }
                        assertEquals(0, getSubscribedChannelsCount(it, queueName)) {"There should be no subscribed channels"}
                    } finally {
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
                        prefetchCount = DEFAULT_PREFETCH_COUNT,
                        confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
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

                            Thread.sleep(2500)
                            LOGGER.info { "Unsubscribing..." }
                            monitor!!.unsubscribe()
                        }
                        for (i in 1..5) {
                            putMessageInQueue(it, queueName)
                            Thread.sleep(1000)
                        }

                        assertEquals(0, getSubscribedChannelsCount(it, queueName)) {"There should be no subscribed channels"}

                        assertEquals(3, counter.get()) { "Wrong number of received messages" }
                        assertTrue(
                            getQueuesInfo(it).toString().contains("$queueName\t2")
                        ) { "There should be messages in the queue" }
                    } finally {
                        Assertions.assertNotNull(thread)
                        Assertions.assertDoesNotThrow {
                            thread!!.interrupt()
                        }
                        assertFalse(thread!!.isAlive)
                    }

                }
            }
    }

    @Test
    fun `connection manager handles ack timeout (multiple subscribers in parallel)`() {
        val configFilename = "rabbitmq_it.conf"

        class Counters {
            val messages = AtomicInteger(0)
            val redeliveredMessages = AtomicInteger(0)
        }

        class ConsumerParams(
            val unsubscribe: Boolean,
            val expectedReceivedMessages: Int,
            val expectedRedeliveredMessages: Int
        )

        class TestParams(
            val queueName: String,
            val subscriberName: String,
            val consumers: List<ConsumerParams>,
            val messagesToSend: Int,
            val expectedChannelsCount: Int,
            val expectedLeftMessages: Int
        )

        val testCases = listOf(
            TestParams(
                queueName = "queue1",
                subscriberName = "subscriber1",
                consumers = listOf(
                    ConsumerParams(
                        unsubscribe = false,
                        expectedReceivedMessages = 3,
                        expectedRedeliveredMessages = 1
                    )
                ),
                expectedChannelsCount = 1,
                messagesToSend = 2,
                expectedLeftMessages = 0
            ),

            TestParams(
                queueName = "queue2",
                subscriberName = "subscriber2",
                consumers = listOf(
                    ConsumerParams(
                        unsubscribe = true,
                        expectedReceivedMessages = 2,
                        expectedRedeliveredMessages = 0
                    )
                ),
                expectedChannelsCount = 0,
                messagesToSend = 2,
                expectedLeftMessages = 1
            )
        )

        RabbitMQContainer(RABBITMQ_IMAGE_NAME)
            .withRabbitMQConfig(MountableFile.forClasspathResource(configFilename))
            .apply { testCases.forEach { withQueue(it.queueName) } }
            .use { container ->
                container.start()
                LOGGER.info { "Started with port ${container.amqpPort}" }

                class TestCaseContext(
                    val connectionManager: ConnectionManager,
                    val consumersThreads: List<Thread>,
                    val consumerCounters: List<Counters>
                )

                val testCasesContexts: List<TestCaseContext> = testCases.map { params ->
                    val connectionManager = createConnectionManager(
                        container,
                        ConnectionManagerConfiguration(
                            subscriberName = params.subscriberName,
                            prefetchCount = DEFAULT_PREFETCH_COUNT,
                            confirmationTimeout = DEFAULT_CONFIRMATION_TIMEOUT,
                            minConnectionRecoveryTimeout = 100,
                            maxConnectionRecoveryTimeout = 200,
                            maxRecoveryAttempts = 5
                        )
                    )

                    val consumerCounters: List<Counters> = List(params.consumers.size) { Counters() }

                    class DeliverCallback(private val consumerNumber: Int) : ManualAckDeliveryCallback {
                        override fun handle(
                            deliveryMetadata: DeliveryMetadata,
                            delivery: Delivery,
                            confirmProcessed: ManualAckDeliveryCallback.Confirmation
                        ) {
                            val consumerCounter = consumerCounters[consumerNumber]

                            LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from \"${delivery.envelope.routingKey}\"" }
                            if (consumerCounter.messages.getAndIncrement() == 1) {
                                LOGGER.info { "Left this message unacked" }
                            } else {
                                confirmProcessed.confirm()
                                LOGGER.info { "Confirmed!" }
                            }

                            if (delivery.envelope.isRedeliver) {
                                consumerCounter.redeliveredMessages.incrementAndGet()
                            }
                        }
                    }

                    val consumersThreads = params.consumers.mapIndexed { index, subscriberParams ->
                        thread {
                            val subscriberMonitor = connectionManager.basicConsume(params.queueName, DeliverCallback(index)) {
                                LOGGER.info { "Canceled $it" }
                            }

                            Thread.sleep(1000)

                            if (subscriberParams.unsubscribe) {
                                LOGGER.info { "Unsubscribing..." }
                                subscriberMonitor.unsubscribe()
                            }
                        }
                    }

                    repeat(params.messagesToSend) {
                        LOGGER.info { "Sending message ${it + 1} to queue ${params.queueName}" }
                        putMessageInQueue(container, params.queueName)
                    }

                    TestCaseContext(connectionManager, consumersThreads, consumerCounters)
                }

                LOGGER.info { "Sleeping..." }
                Thread.sleep(63000)

                val queuesListExecResult = getQueuesInfo(container)
                LOGGER.info { "queues list: \n $queuesListExecResult" }

                testCases.forEachIndexed { index, params ->
                    val context = testCasesContexts[index]
                    assertEquals(params.expectedChannelsCount, getSubscribedChannelsCount(container, params.queueName)) {
                        "Wrong number of opened channels (subscriber: `${params.subscriberName}`)"
                    }

                    params.consumers.forEachIndexed { consumerIndex, consumerParams ->
                        val counters = context.consumerCounters[consumerIndex]
                        assertEquals(consumerParams.expectedReceivedMessages, counters.messages.get()) {
                            "Wrong number of received messages (subscriber: `${params.subscriberName}`, consumer index: `${consumerIndex}`)"
                        }

                        assertEquals(consumerParams.expectedRedeliveredMessages, counters.redeliveredMessages.get()) {
                            "Wrong number of redelivered messages (subscriber: `${params.subscriberName}`, consumer index: `${consumerIndex}`)"
                        }
                    }

                    assertTrue(queuesListExecResult.toString().contains("${params.queueName}\t${params.expectedLeftMessages}")) {
                        "There should ${params.expectedLeftMessages} message(s) left in the '${params.queueName}' queue"
                    }
                }

                testCasesContexts.forEach { context ->
                    context.consumersThreads.forEach { it.interrupt() }
                    context.connectionManager.close()
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

    private fun <T> assertTarget(target: T, timeout: Long = 1_000, message: String, func: () -> T) {
        val start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeout) {
            if (func() == target) {
                return
            }
            Thread.sleep(100)
        }
        assertEquals(target, func(), message)
    }

    private fun createConnectionManager(container: RabbitMQContainer, configuration: ConnectionManagerConfiguration) =
        ConnectionManager(
            "test-connection",
            RabbitMQConfiguration(
                host = container.host,
                vHost = "",
                port = container.amqpPort,
                username = container.adminUsername,
                password = container.adminPassword,
            ),
            configuration
        )

    @Test
    fun `connection manager exclusive queue test`() {
        RabbitMQContainer(RABBITMQ_IMAGE_NAME).use { rabbitMQContainer ->
            rabbitMQContainer.start()
            LOGGER.info { "Started with port ${rabbitMQContainer.amqpPort}" }

            createConnectionManager(rabbitMQContainer).use { firstManager ->
                createConnectionManager(rabbitMQContainer).use { secondManager ->
                    val queue = firstManager.queueDeclare()

                    assertFailsWith<IOException>("Another connection can subscribe to the $queue queue") {
                        secondManager.basicConsume(queue, { _, _, _ -> }, {})
                    }

                    extracted(firstManager, secondManager, queue, 3)
                    extracted(firstManager, secondManager, queue, 6)
                }
            }
        }
    }

    private fun extracted(
        firstManager: ConnectionManager,
        secondManager: ConnectionManager,
        queue: String,
        cycle: Int
    ) {
        val countDown = CountDownLatch(cycle)
        val deliverCallback = ManualAckDeliveryCallback { _, delivery, conformation ->
            countDown.countDown()
            LOGGER.info { "Received ${delivery.body.toString(Charsets.UTF_8)} from ${delivery.envelope.exchange}:${delivery.envelope.routingKey}, received ${countDown.count}" }
            conformation.confirm()
        }
        val cancelCallback = CancelCallback { LOGGER.warn { "Canceled $it" } }

        val firstMonitor = firstManager.basicConsume(queue, deliverCallback, cancelCallback)
        val secondMonitor = firstManager.basicConsume(queue, deliverCallback, cancelCallback)

        repeat(cycle) { index ->
            secondManager.basicPublish(
                "",
                queue,
                null,
                "Hello $index".toByteArray(Charsets.UTF_8)
            )
        }

        assertTrue(
            countDown.await(
                1L,
                TimeUnit.SECONDS
            )
        ) { "Not all messages were received: ${countDown.count}" }

        assertTrue(firstManager.isAlive) { "Manager should still be alive" }
        assertTrue(firstManager.isReady) { "Manager should be ready until the confirmation timeout expires" }

        firstMonitor.unsubscribe()
        secondMonitor.unsubscribe()
    }

    private fun createConnectionManager(
        rabbitMQContainer: RabbitMQContainer,
        prefetchCount: Int = DEFAULT_PREFETCH_COUNT,
        confirmationTimeout: Duration = DEFAULT_CONFIRMATION_TIMEOUT,
    ) = ConnectionManager(
        "test-connection",
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
            confirmationTimeout = confirmationTimeout
        )
    )

    @AfterEach
    fun cleanupRabbitMq() {
        // cleanup is done to prevent queue name collision during test
        rabbit.apply {
            executeInContainerWithLogging("rabbitmqctl", "stop_app")
            executeInContainerWithLogging("rabbitmqctl", "reset")
            executeInContainerWithLogging("rabbitmqctl", "start_app")
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private lateinit var rabbit: RabbitMQContainer

        @JvmStatic
        fun GenericContainer<*>.executeInContainerWithLogging(vararg command: String, exceptionOnExecutionError: Boolean = true) {
            execInContainer(*command).also {
                LOGGER.info { "Command: ${command.joinToString(separator = " ")}; out: ${it.stdout}; err: ${it.stderr}; exit code: ${it.exitCode}" }
                if (exceptionOnExecutionError && it.exitCode != 0) {
                    throw IllegalStateException("Command ${command.joinToString()} exited with error code: ${it.exitCode}")
                }
            }
        }

        @BeforeAll
        @JvmStatic
        fun initRabbit() {
            rabbit = RabbitMQContainer(RABBITMQ_IMAGE_NAME)
                .withLogConsumer(Slf4jLogConsumer(LoggerFactory.getLogger("rabbitmq")))
                .withCreateContainerCmdModifier {
                    it.hostConfig
                        // required to use tc tool to emulate network problems
                        ?.withCapAdd(Capability.NET_ADMIN)
                }
            rabbit.start()
        }

        @AfterAll
        @JvmStatic
        fun closeRabbit() {
            rabbit.close()
        }
    }
}
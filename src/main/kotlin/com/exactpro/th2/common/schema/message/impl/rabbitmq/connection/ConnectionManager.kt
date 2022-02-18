/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import java.lang.Runnable
import java.lang.AutoCloseable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import com.exactpro.th2.common.metrics.HealthMetrics
import com.rabbitmq.client.RecoveryListener
import com.rabbitmq.client.Recoverable
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.TopologyRecoveryException
import java.lang.IllegalStateException
import com.rabbitmq.client.RecoveryDelayHandler
import com.rabbitmq.http.client.ClientParameters
import java.io.IOException
import java.net.URISyntaxException
import com.rabbitmq.client.BlockedListener
import kotlin.Throws
import java.util.concurrent.TimeUnit
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.CancelCallback
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import java.lang.RuntimeException
import java.lang.InterruptedException
import java.util.function.BiConsumer
import com.rabbitmq.client.ShutdownNotifier
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Consumer
import com.rabbitmq.client.ExceptionHandler
import com.rabbitmq.http.client.Client
import com.rabbitmq.http.client.domain.DestinationType
import mu.KotlinLogging
import org.apache.commons.lang3.builder.EqualsBuilder
import org.apache.commons.lang3.builder.HashCodeBuilder
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.commons.lang3.builder.ToStringStyle
import java.lang.NullPointerException
import java.util.Objects
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier

class ConnectionManager(
    private val rabbitMQConfiguration: RabbitMQConfiguration,
    val connectionManagerConfiguration: ConnectionManagerConfiguration,
    onFailedRecoveryConnection: Runnable?,
) : AutoCloseable {
    private val connection: Connection
    private val channelsByPin: MutableMap<PinId, ChannelHolder> = ConcurrentHashMap()
    private val connectionRecoveryAttempts = AtomicInteger(0)
    private val connectionIsClosed = AtomicBoolean(false)

    private val subscriberName: String
    private val nextSubscriberId = AtomicInteger(1)
    private val sharedExecutor = Executors.newSingleThreadExecutor(ThreadFactoryBuilder()
        .setNameFormat("rabbitmq-shared-pool-%d")
        .build())
    private val client: Client
    private val sizeCheckExecutor = Executors.newScheduledThreadPool(1)
    private val knownExchangesToRoutingKeys: MutableMap<String, MutableSet<String>> = ConcurrentHashMap()
    private val metrics = HealthMetrics(this)
    private val recoveryListener: RecoveryListener = object : RecoveryListener {
        override fun handleRecovery(recoverable: Recoverable) {
            LOGGER.debug("Count tries to recovery connection reset to 0")
            connectionRecoveryAttempts.set(0)
            metrics.readinessMonitor.enable()
            LOGGER.debug("Set RabbitMQ readiness to true")
        }

        override fun handleRecoveryStarted(recoverable: Recoverable) {}
    }

    init {
        val subscriberNameTmp: String? = connectionManagerConfiguration.subscriberName
            ?: rabbitMQConfiguration.subscriberName
        if (subscriberNameTmp == null || subscriberNameTmp.isBlank()) {
            subscriberName = "rabbit_mq_subscriber." + System.currentTimeMillis()
            LOGGER.info("Subscribers will use default name: {}", subscriberName)
        } else {
            subscriberName = subscriberNameTmp + "." + System.currentTimeMillis()
        }
        val factory = ConnectionFactory().apply {
            host = rabbitMQConfiguration.host
            port = rabbitMQConfiguration.port
        }
        val virtualHost = rabbitMQConfiguration.vHost
        if (virtualHost.isNotBlank()) {
            factory.virtualHost = virtualHost
        }
        val username = rabbitMQConfiguration.username
        if (username.isNotBlank()) {
            factory.username = username
        }
        val password = rabbitMQConfiguration.password
        if (password.isNotBlank()) {
            factory.password = password
        }
        if (connectionManagerConfiguration.connectionTimeout > 0) {
            factory.connectionTimeout = connectionManagerConfiguration.connectionTimeout
        }
        factory.exceptionHandler = object : ExceptionHandler {
            override fun handleUnexpectedConnectionDriverException(conn: Connection, exception: Throwable) {
                turnOffReadiness(exception)
            }

            override fun handleReturnListenerException(channel: Channel, exception: Throwable) {
                turnOffReadiness(exception)
            }

            override fun handleConfirmListenerException(channel: Channel, exception: Throwable) {
                turnOffReadiness(exception)
            }

            override fun handleBlockedListenerException(connection: Connection, exception: Throwable) {
                turnOffReadiness(exception)
            }

            override fun handleConsumerException(
                channel: Channel,
                exception: Throwable,
                consumer: Consumer,
                consumerTag: String,
                methodName: String,
            ) {
                turnOffReadiness(exception)
            }

            override fun handleConnectionRecoveryException(conn: Connection, exception: Throwable) {
                turnOffReadiness(exception)
            }

            override fun handleChannelRecoveryException(ch: Channel, exception: Throwable) {
                turnOffReadiness(exception)
            }

            override fun handleTopologyRecoveryException(
                conn: Connection,
                ch: Channel,
                exception: TopologyRecoveryException,
            ) {
                turnOffReadiness(exception)
            }

            private fun turnOffReadiness(exception: Throwable) {
                metrics.readinessMonitor.disable()
                LOGGER.debug("Set RabbitMQ readiness to false. RabbitMQ error", exception)
            }
        }
        factory.isAutomaticRecoveryEnabled = true
        factory.setConnectionRecoveryTriggeringCondition {
            if (connectionIsClosed.get()) {
                return@setConnectionRecoveryTriggeringCondition false
            }
            val tmpCountTriesToRecovery = connectionRecoveryAttempts.get()
            if (tmpCountTriesToRecovery < connectionManagerConfiguration.maxRecoveryAttempts) {
                LOGGER.info("Try to recovery connection to RabbitMQ. Count tries = {}", tmpCountTriesToRecovery + 1)
                return@setConnectionRecoveryTriggeringCondition true
            }
            LOGGER.error("Can not connect to RabbitMQ. Count tries = {}", tmpCountTriesToRecovery)
            if (onFailedRecoveryConnection != null) {
                onFailedRecoveryConnection.run()
            } else {
                // TODO: we should stop the execution of the application. Don't use System.exit!!!
                throw IllegalStateException("Cannot recover connection to RabbitMQ")
            }
            false
        }
        factory.recoveryDelayHandler = RecoveryDelayHandler {
            val tmpCountTriesToRecovery = connectionRecoveryAttempts.getAndIncrement()
            val recoveryDelay = (connectionManagerConfiguration.minConnectionRecoveryTimeout
                    + if (connectionManagerConfiguration.maxRecoveryAttempts > 1) (connectionManagerConfiguration.maxConnectionRecoveryTimeout - connectionManagerConfiguration.minConnectionRecoveryTimeout)
                    / (connectionManagerConfiguration.maxRecoveryAttempts - 1)
                    * tmpCountTriesToRecovery else 0)
            LOGGER.info("Recovery delay for '{}' try = {}", tmpCountTriesToRecovery, recoveryDelay)
            recoveryDelay.toLong()
        }
        factory.setSharedExecutor(sharedExecutor)
        try {
            connection = factory.newConnection()
            client = Client(
                ClientParameters()
                    .url(String.format(RABBITMQ_MANAGEMENT_URL, rabbitMQConfiguration.host))
                    .username(rabbitMQConfiguration.username)
                    .password(rabbitMQConfiguration.password)
            )
            metrics.readinessMonitor.enable()
            LOGGER.debug("Set RabbitMQ readiness to true")
        } catch (e: IOException) {
            metrics.readinessMonitor.disable()
            LOGGER.debug("Set RabbitMQ readiness to false. Can not create connection", e)
            throw IllegalStateException("Failed to create RabbitMQ connection using configuration", e)
        } catch (e: TimeoutException) {
            metrics.readinessMonitor.disable()
            LOGGER.debug("Set RabbitMQ readiness to false. Can not create connection", e)
            throw IllegalStateException("Failed to create RabbitMQ connection using configuration", e)
        } catch (e: URISyntaxException) {
            metrics.readinessMonitor.disable()
            LOGGER.debug("Set RabbitMQ readiness to false. Can not create connection", e)
            throw IllegalStateException("Failed to create RabbitMQ connection using configuration", e)
        }
        connection.addBlockedListener(object : BlockedListener {
            @Throws(IOException::class)
            override fun handleBlocked(reason: String) {
                LOGGER.warn("RabbitMQ blocked connection: {}", reason)
            }

            @Throws(IOException::class)
            override fun handleUnblocked() {
                LOGGER.warn("RabbitMQ unblocked connection")
            }
        })
        if (connection is Recoverable) {
            (connection as Recoverable).apply {
                addRecoveryListener(recoveryListener)
            }
            LOGGER.debug("Recovery listener was added to connection.")
        } else {
            throw IllegalStateException("Connection does not implement Recoverable. Can not add RecoveryListener to it")
        }
        sizeCheckExecutor.scheduleAtFixedRate(
            ::lockSendingIfSizeLimitExceeded,
            connectionManagerConfiguration.secondsToCheckVirtualPublishLimit.toLong(),  // TODO another initial delay?
            connectionManagerConfiguration.secondsToCheckVirtualPublishLimit.toLong(),
            TimeUnit.SECONDS
        )
    }

    override fun close() {
        if (connectionIsClosed.getAndSet(true)) {
            return
        }
        val closeTimeout = connectionManagerConfiguration.connectionCloseTimeout
        if (connection.isOpen) {
            try {
                // We close the connection and don't close channels
                // because when a channel's connection is closed, so is the channel
                connection.close(closeTimeout)
            } catch (e: IOException) {
                LOGGER.error("Cannot close connection", e)
            }
        }
        shutdownExecutor(sharedExecutor, closeTimeout)
        shutdownExecutor(sizeCheckExecutor, closeTimeout)
    }

    @Throws(IOException::class)
    fun basicPublish(exchange: String, routingKey: String, props: AMQP.BasicProperties?, body: ByteArray?) {
        knownExchangesToRoutingKeys.computeIfAbsent(exchange) { mutableSetOf() }.add(routingKey)
        getChannelFor(PinId.forRoutingKey(routingKey)).publishWithLocks(exchange, routingKey, props, body)
    }

    @Throws(IOException::class)
    fun basicConsume(
        queue: String,
        deliverCallback: DeliverCallback,
        cancelCallback: CancelCallback?,
    ): SubscriberMonitor {
        val holder = getChannelFor(PinId.forQueue(queue))
        val tag = holder.mapWithLock {
            it.basicConsume(
                queue,
                false,
                subscriberName + "_" + nextSubscriberId.getAndIncrement(),
                { tagTmp, delivery ->
                    try {
                        try {
                            deliverCallback.handle(tagTmp, delivery)
                        } finally {
                            holder.withLock { channel ->
                                basicAck(channel, delivery.envelope.deliveryTag)
                            }
                        }
                    } catch (e: IOException) {
                        LOGGER.error("Cannot handle delivery for tag {}: {}", tagTmp, e.message, e)
                    } catch (e: RuntimeException) {
                        LOGGER.error("Cannot handle delivery for tag {}: {}", tagTmp, e.message, e)
                    }
                },
                cancelCallback
            )
        }
        return SubscriberMonitor { holder.withLock(false) { it.basicCancel(tag) } }
    }

    private fun shutdownExecutor(executor: ExecutorService, closeTimeout: Int) {
        executor.shutdown()
        try {
            if (!executor.awaitTermination(closeTimeout.toLong(), TimeUnit.MILLISECONDS)) {
                LOGGER.error("Executor is not terminated during {} millis", closeTimeout)
                val runnables = executor.shutdownNow()
                LOGGER.error("{} task(s) was(were) not finished", runnables.size)
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    private fun getChannelFor(pinId: PinId) = channelsByPin.computeIfAbsent(pinId) {
        LOGGER.trace("Creating channel holder for {}", pinId)
        ChannelHolder(::createChannel, ::waitForConnectionRecovery)
    }

    private fun createChannel(): Channel {
        waitForConnectionRecovery(connection)
        return try {
            val channel = connection.createChannel()
            Objects.requireNonNull(channel) { "No channels are available in the connection. Max channel number: " + connection.channelMax }
            channel.basicQos(connectionManagerConfiguration.prefetchCount)
            channel.addReturnListener { ret ->
                LOGGER.warn("Can not router message to exchange '{}', routing key '{}'. Reply code '{}' and text = {}",
                    ret.exchange,
                    ret.routingKey,
                    ret.replyCode,
                    ret.replyText)
            }
            channel
        } catch (e: IOException) {
            throw IllegalStateException("Can not create channel", e)
        }
    }

    private fun waitForConnectionRecovery(notifier: ShutdownNotifier, waitForRecovery: Boolean = true) {
        if (isConnectionRecovery(notifier)) {
            if (waitForRecovery) {
                waitForRecovery(notifier)
            } else {
                LOGGER.warn("Skip waiting for connection recovery")
            }
        }
        check(!connectionIsClosed.get()) { "Connection is already closed" }
    }

    private fun waitForRecovery(notifier: ShutdownNotifier) {
        LOGGER.warn("Start waiting for connection recovery")
        while (isConnectionRecovery(notifier)) {
            try {
                Thread.sleep(1)
            } catch (e: InterruptedException) {
                LOGGER.error("Wait for connection recovery was interrupted", e)
                break
            }
        }
        LOGGER.info("Stop waiting for connection recovery")
    }

    private fun isConnectionRecovery(notifier: ShutdownNotifier): Boolean {
        return !notifier.isOpen && !connectionIsClosed.get()
    }

    /**
     * @param channel pass channel witch used for basicConsume, because delivery tags are scoped per channel,
     * deliveries must be acknowledged on the same channel they were received on.
     * @throws IOException
     */
    @Throws(IOException::class)
    private fun basicAck(channel: Channel, deliveryTag: Long) {
        channel.basicAck(deliveryTag, false)
    }

    fun lockSendingIfSizeLimitExceeded() = try {
        val queueNameToSize = client.queues
            .associateBy({ it.name }, { it.totalMessages })
        knownExchangesToRoutingKeys.entries.asSequence()
            .flatMap { (exchange, routingKeys) ->
                client.getBindingsBySource(rabbitMQConfiguration.vHost, exchange).asSequence()
                    .filter { it.destinationType == DestinationType.QUEUE && routingKeys.contains(it.routingKey) }
            }
            .groupBy { it.routingKey }
            .forEach { (routingKey, bindings) ->
                val bindingNameToSize = bindings
                    .associateBy({ it.destination }, { queueNameToSize.getValue(it.destination) })
                val limit = connectionManagerConfiguration.virtualPublishLimit
                val holder = getChannelFor(PinId.forRoutingKey(routingKey))
                val sizeDetails = {
                    bindingNameToSize.entries
                        .joinToString { "${it.value} message(s) in '${it.key}'" }
                }
                if (bindingNameToSize.values.sum() > limit) {
                    if (!holder.sizeLimitLock.isLocked) {
                        holder.sizeLimitLock.lock()
                        LOGGER.info { "Sending via routing key '$routingKey' is paused because there are ${sizeDetails()}. Virtual publish limit is $limit" }
                    }
                } else {
                    if (holder.sizeLimitLock.isLocked) {
                        holder.sizeLimitLock.unlock()
                        LOGGER.info { "Sending via routing key '$routingKey' is resumed. There are ${sizeDetails()}. Virtual publish limit is $limit" }
                    }
                }
            }
    } catch (t: Throwable) {
        LOGGER.error("Error during check queue sizes", t)
    }

    private class PinId private constructor(routingKey: String?, queue: String?) {
        private val routingKey: String?
        private val queue: String?

        init {
            if (routingKey == null && queue == null) {
                throw NullPointerException("Either routingKey or queue must be set")
            }
            this.routingKey = routingKey
            this.queue = queue
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other == null || javaClass != other.javaClass) return false
            val pinId = other as PinId
            return EqualsBuilder()
                .append(routingKey, pinId.routingKey)
                .append(queue, pinId.queue)
                .isEquals
        }

        override fun hashCode(): Int = HashCodeBuilder(17, 37)
            .append(routingKey)
            .append(queue)
            .toHashCode()

        override fun toString(): String = ToStringBuilder(this, ToStringStyle.JSON_STYLE)
            .append("routingKey", routingKey)
            .append("queue", queue)
            .toString()

        companion object {
            fun forRoutingKey(routingKey: String): PinId {
                return PinId(routingKey, null)
            }

            fun forQueue(queue: String): PinId {
                return PinId(null, queue)
            }
        }
    }

    private class ChannelHolder(
        private val supplier: Supplier<Channel>,
        private val reconnectionChecker: BiConsumer<ShutdownNotifier, Boolean>,
    ) {
        val sizeLimitLock = ReentrantLock()
        private val lock: Lock = ReentrantLock()
        private var channel: Channel? = null

        @Throws(IOException::class)
        fun publishWithLocks(exchange: String, routingKey: String, props: AMQP.BasicProperties?, body: ByteArray?) {
            sizeLimitLock.lock()
            try {
                withLock(true) { it.basicPublish(exchange, routingKey, props, body) }
            } finally {
                sizeLimitLock.unlock()
            }
        }

        @Throws(IOException::class)
        fun withLock(consumer: (Channel) -> Unit) {
            withLock(true, consumer)
        }

        @Throws(IOException::class)
        fun withLock(waitForRecovery: Boolean, consumer: (Channel) -> Unit) {
            lock.lock()
            try {
                consumer(getChannel(waitForRecovery))
            } finally {
                lock.unlock()
            }
        }

        @Throws(IOException::class)
        fun <T> mapWithLock(mapper: (Channel) -> T): T {
            lock.lock()
            return try {
                mapper(getChannel())
            } finally {
                lock.unlock()
            }
        }

        private fun getChannel(): Channel {
            return getChannel(true)
        }

        private fun getChannel(waitForRecovery: Boolean): Channel {
            if (channel == null) {
                channel = supplier.get()
            }
            reconnectionChecker.accept(channel!!, waitForRecovery)
            return channel!!
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private const val RABBITMQ_MANAGEMENT_URL = "http://%s:15672/api/"
    }
}
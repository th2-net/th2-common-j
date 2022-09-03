/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.exactpro.th2.common.schema.message.impl.rabbitmq

import com.exactpro.th2.common.schema.exception.RouterException
import com.exactpro.th2.common.schema.filter.strategy.FilterStrategy
import com.exactpro.th2.common.schema.message.*
import com.exactpro.th2.common.schema.message.QueueAttribute.PUBLISH
import com.exactpro.th2.common.schema.message.QueueAttribute.SUBSCRIBE
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.google.protobuf.Message
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

typealias PinName = String
typealias PinConfiguration = QueueConfiguration
typealias Queue = String
typealias RoutingKey = String

abstract class AbstractRabbitRouter<T> : MessageRouter<T> {
    private val _context = AtomicReference<MessageRouterContext?>()
    protected var context: MessageRouterContext
        get() = checkNotNull(_context.get()) { "Router didn't initialized yet" }
        private set(context) = check(_context.compareAndSet(null, context)) {
            "Router is already initialized"
        }

    private val configuration: MessageRouterConfiguration
        get() = context.configuration

    protected val connectionManager: ConnectionManager
        get() = context.connectionManager

    private val subscribers = ConcurrentHashMap<Queue, MessageSubscriber<T>>()
    private val senders = ConcurrentHashMap<RoutingKey, MessageSender<T>>()

    private val filterStrategy = AtomicReference(getDefaultFilterStrategy())

    protected open fun getDefaultFilterStrategy(): FilterStrategy<Message> {
        return FilterStrategy.DEFAULT_FILTER_STRATEGY
    }

    protected open fun filterMessage(msg: Message, filters: List<RouterFilter>): Boolean {
        return filterStrategy.get().verify(msg, filters)
    }

    override fun init(context: MessageRouterContext) {
        this.context = context
    }

    override fun send(message: T, vararg attributes: String) {
        val pintAttributes: Set<String> = appendAttributes(*attributes) { getRequiredSendAttributes() }
        send(message, pintAttributes) {
            check(size == 1 || (isNotEmpty() && oneOrNoneWithData())) {
                "Found incorrect number of pins ${map(PinInfo::pinName)} to the send operation by attributes $pintAttributes and filters, expected 1, actual $size"
            }
        }
    }

    override fun sendAll(message: T, vararg attributes: String) {
        val pintAttributes: Set<String> = appendAttributes(*attributes) { getRequiredSendAttributes() }
        send(message, pintAttributes) {
            check(isNotEmpty()) {
                "Found incorrect number of pins ${map(PinInfo::pinName)} to send all operation by attributes $pintAttributes and filters, expected 1 or more, actual $size"
            }
        }
    }

    override fun subscribe(callback: MessageListener<T>, vararg attributes: String): SubscriberMonitor {
        return subscribeWithManualAck(ConfirmationMessageListener.wrap(callback), *attributes)
    }

    override fun subscribeAll(callback: MessageListener<T>, vararg attributes: String): SubscriberMonitor {
        return subscribeAllWithManualAck(ConfirmationMessageListener.wrap(callback), *attributes)
    }

    override fun subscribeWithManualAck(callback: ConfirmationMessageListener<T>, vararg attributes: String): SubscriberMonitor {
        val pintAttributes: Set<String> = appendAttributes(*attributes) { getRequiredSubscribeAttributes() }
        return subscribe(pintAttributes, callback) {
            check(size == 1) {
                "Found incorrect number of pins ${map(PinInfo::pinName)} to subscribe operation by attributes $pintAttributes and filters, expected 1, actual $size"
            }
        }
    }

    override fun subscribeAllWithManualAck(callback: ConfirmationMessageListener<T>, vararg attributes: String): SubscriberMonitor {
        val pintAttributes: Set<String> = appendAttributes(*attributes) { getRequiredSubscribeAttributes() }
        return subscribe(pintAttributes, callback) {
            check(isNotEmpty()) {
                "Found incorrect number of pins ${map(PinInfo::pinName)} to subscribe all operation by attributes $pintAttributes and filters, expected 1 or more, actual $size"
            }
        }
    }

    override fun close() {
        LOGGER.info("Closing message router")

        val exceptions = mutableListOf<Throwable>()
        subscribers.values.forEach { subscriber ->
            runCatching(subscriber::close)
                .onFailure { exceptions.add(it) }
        }
        subscribers.clear()

        checkOrThrow(exceptions) { "Can not close message router" }
        LOGGER.info("Message router has been successfully closed")
    }

    /**
     * Prepares the message to send via the specified pin.
     * An implementation can rebuild the passed message according to pin configuration and returns a new instance of a message.
     * @param message a source message which can be reduced according to pin configuration
     * @return the source message, part of them, or null if the message is matched to the pin configuration: fully, partially, not match accordingly
     */
    protected abstract fun splitAndFilter(message: T, pinConfiguration: PinConfiguration, pinName: PinName): T?

    /**
     * Returns default set of attributes for send operations
     */
    protected open fun getRequiredSendAttributes() = REQUIRED_SEND_ATTRIBUTES

    /**
     * Returns default set of attributes for subscribe operations
     */
    protected open fun getRequiredSubscribeAttributes() = REQUIRED_SUBSCRIBE_ATTRIBUTES

    //TODO: implement common sender
    protected abstract fun createSender(pinConfig: PinConfiguration, pinName: PinName): MessageSender<T>

    //TODO: implement common subscriber
    protected abstract fun createSubscriber(pinConfig: PinConfiguration, pinName: PinName): MessageSubscriber<T>

    protected abstract fun T.toErrorString(): String

    private fun send(
        message: T, pintAttributes: Set<String>,
        check: List<PinPublication<T>>.() -> Unit
    ) {
        val packages: List<PinPublication<T>> = configuration.queues.asSequence()
            .filter { it.value.attributes.containsAll(pintAttributes) }
            .map { (pinName, pinConfig) ->
                PinPublication(pinName, pinConfig, splitAndFilter(message, pinConfig, pinName))
            }
            .toList()
            .also(check)

        val exceptions: MutableMap<PinName, Throwable> = mutableMapOf()
        packages.forEach { (pinName: PinName, pinConfig: PinConfiguration, message: T?) ->
            if (message == null) {
                LOGGER.debug { "Publication to pin $pinName with attributes ${pinConfig.attributes} was dropped because all content was filtered" }
                return@forEach
            }
            try {
                senders.getSender(pinName, pinConfig)
                    .send(message)
            } catch (e: Exception) {
                LOGGER.error(e) { "Message ${message.toErrorString()} can't be send through the $pinName pin" }
                exceptions[pinName] = e
            }
        }
        checkOrThrow(exceptions.values) { "Can't send to pin(s): ${exceptions.keys}" }
    }

    private fun subscribe(
        pintAttributes: Set<String>,
        messageListener: ConfirmationMessageListener<T>,
        check: List<PinInfo>.() -> Unit
    ): SubscriberMonitor {
        val packages: List<PinInfo> = configuration.queues.asSequence()
            .filter { it.value.attributes.containsAll(pintAttributes) }
            .map { (pinName, pinConfig) -> PinInfo(pinName, pinConfig) }
            .toList()
            .also(check)

        //TODO: catch exceptions during subscriptions and roll back
        val exceptions: MutableMap<PinName, Throwable> = mutableMapOf()
        val monitors: MutableList<SubscriberMonitor> = mutableListOf()
        packages.forEach { (pinName: PinName, pinConfig: PinConfiguration) ->
            runCatching {
                subscribers.getSubscriber(pinName, pinConfig).apply {
                    addListener(messageListener)
                    start() //TODO: replace to lazy start on add listener(s)
                }
            }.onFailure { e ->
                LOGGER.error(e) { "Listener can't be subscribed via the $pinName pin" }
                exceptions[pinName] = e
            }.onSuccess {
                monitors.add(SubscriberMonitor { close() })
            }
        }

        checkOrThrow(exceptions.values) { "Can't subscribe to pin(s): ${exceptions.keys}" }

        return when (monitors.size) {
            1 -> monitors[0]
            else -> SubscriberMonitor {
                monitors.forEach(SubscriberMonitor::unsubscribe)
            }
        }
    }

    private inline fun checkOrThrow(exceptions: Collection<Throwable>, message: () -> String) {
        if (exceptions.isNotEmpty()) {
            throw RouterException(message()).apply {
                exceptions.forEach(this::addSuppressed)
            }
        }
    }

    private fun ConcurrentHashMap<RoutingKey, MessageSender<T>>.getSender(
        pinName: PinName,
        pinConfig: PinConfiguration
    ): MessageSender<T> = computeIfAbsent(pinConfig.routingKey) {
        check(pinConfig.isWritable) {
            "The $pinName isn't writable, configuration: $pinConfig"
        }

        return@computeIfAbsent createSender(pinConfig, pinName)
    }

    private fun ConcurrentHashMap<Queue, MessageSubscriber<T>>.getSubscriber(
        pinName: PinName,
        pinConfig: PinConfiguration
    ): MessageSubscriber<T> = computeIfAbsent(pinConfig.queue) {
        check(pinConfig.isReadable) {
            "The $pinName isn't readable, configuration: $pinConfig"
        }

        return@computeIfAbsent createSubscriber(pinConfig, pinName)
    }

    private open class PinInfo(
        val pinName: PinName,
        val pinConfig: PinConfiguration
    ) {
        operator fun component1(): PinName = pinName
        operator fun component2(): PinConfiguration = pinConfig

        override fun toString(): String {
            return "pinName=$pinName; pinConfig=$pinConfig"
        }
    }

    private class PinPublication<T>(
        pinName: PinName,
        pinConfig: PinConfiguration,
        val data: T?
    ) : PinInfo(pinName, pinConfig) {
        operator fun component3(): T? = data

        override fun toString(): String {
            return "${super.toString()}; data=$data"
        }
    }

    private fun List<PinPublication<*>>.oneOrNoneWithData(): Boolean {
        return count { it.data != null } <= 1
    }

    companion object {

        private val LOGGER = KotlinLogging.logger {}
        private val REQUIRED_SEND_ATTRIBUTES = setOf(PUBLISH.toString())
        private val REQUIRED_SUBSCRIBE_ATTRIBUTES = setOf(SUBSCRIBE.toString())
    }
}

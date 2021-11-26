/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.notification

import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.schema.message.FilterFunction
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageSubscriber
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.rabbitmq.client.Delivery
import mu.KotlinLogging
import java.util.concurrent.CopyOnWriteArrayList

class NotificationEventBatchSubscriber(
    private val connectionManager: ConnectionManager,
    private val queue: String
) : MessageSubscriber<EventBatch> {
    private val listeners = CopyOnWriteArrayList<MessageListener<EventBatch>>()
    private lateinit var monitor: SubscriberMonitor

    @Deprecated(
        "Method is deprecated, please use constructor",
        ReplaceWith("NotificationEventBatchSubscriber()")
    )
    override fun init(connectionManager: ConnectionManager, exchangeName: String, subscribeTargets: SubscribeTarget) {
        throw UnsupportedOperationException("Method is deprecated, please use constructor")
    }

    @Deprecated(
        "Method is deprecated, please use constructor",
        ReplaceWith("NotificationEventBatchSubscriber()")
    )
    override fun init(
        connectionManager: ConnectionManager,
        subscribeTarget: SubscribeTarget,
        filterFunc: FilterFunction
    ) {
        throw UnsupportedOperationException("Method is deprecated, please use constructor")
    }

    override fun start() {
        monitor = connectionManager.basicConsume(
            queue,
            { consumerTag: String, delivery: Delivery ->
                for (listener in listeners) {
                    try {
                        listener.handler(consumerTag, EventBatch.parseFrom(delivery.body))
                    } catch (listenerExc: Exception) {
                        LOGGER.warn(
                            "Message listener from class '{}' threw exception",
                            listener.javaClass,
                            listenerExc
                        )
                    }
                }
            },
            { LOGGER.warn("Consuming cancelled for: '{}'", it) }
        )
    }

    override fun addListener(messageListener: MessageListener<EventBatch>) {
        listeners.add(messageListener)
    }

    override fun close() {
        monitor.unsubscribe()
        listeners.forEach(MessageListener<EventBatch>::onClose)
        listeners.clear()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}

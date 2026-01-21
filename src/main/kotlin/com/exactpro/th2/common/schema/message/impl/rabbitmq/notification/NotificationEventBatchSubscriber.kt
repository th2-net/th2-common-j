/*
 * Copyright 2021-2026 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.schema.message.ConfirmationListener
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback
import com.exactpro.th2.common.schema.message.MessageSubscriber
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConsumeConnectionManager
import com.rabbitmq.client.Delivery
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.CopyOnWriteArrayList

// DRAFT of notification router
class NotificationEventBatchSubscriber(
    private val consumeConnectionManager: ConsumeConnectionManager,
    private val queue: String
) : MessageSubscriber {
    private val listeners = CopyOnWriteArrayList<ConfirmationListener<EventBatch>>()
    private lateinit var monitor: SubscriberMonitor

    fun start() {
        monitor = consumeConnectionManager.basicConsume(
            queue,
            { deliveryMetadata: DeliveryMetadata, delivery: Delivery, confirmation: ManualAckDeliveryCallback.Confirmation ->
                try {
                    for (listener in listeners) {
                        try {
                            listener.handle(deliveryMetadata, EventBatch.parseFrom(delivery.body), confirmation)
                        } catch (listenerExc: Exception) {
                            LOGGER.warn(
                                "Message listener from class '{}' threw exception",
                                listener.javaClass,
                                listenerExc
                            )
                        }
                    }
                } finally {
                    confirmation.confirm()
                }
            },
            { LOGGER.warn("Consuming cancelled for: '{}'", it) }
        )
    }

    fun removeListener(messageListener: ConfirmationListener<EventBatch>) {
        listeners.remove(messageListener)
    }

    fun addListener(messageListener: ConfirmationListener<EventBatch>) {
        listeners.add(messageListener)
    }

    override fun close() {
        monitor.unsubscribe()
        listeners.forEach(ConfirmationListener<EventBatch>::onClose)
        listeners.clear()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}
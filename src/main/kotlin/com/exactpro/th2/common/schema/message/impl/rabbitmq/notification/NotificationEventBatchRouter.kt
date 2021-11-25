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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.notification

import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.schema.exception.RouterException
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.MessageRouterContext
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import mu.KotlinLogging

private const val NOTIFICATION_EXCHANGE = "global-notification"
private const val NOTIFICATION_QUEUE = "global-notification-queue"

class NotificationEventBatchRouter : MessageRouter<EventBatch> {
    private lateinit var queue: String
    private lateinit var sender: NotificationEventBatchSender
    private lateinit var subscriber: NotificationEventBatchSubscriber

    override fun init(context: MessageRouterContext) {
        sender = NotificationEventBatchSender(context.connectionManager, NOTIFICATION_EXCHANGE)
        queue = context.connectionManager.queueDeclare(NOTIFICATION_QUEUE)
        LOGGER.info { "Created '$queue' notification queue" }
        context.connectionManager.queueBind(queue, NOTIFICATION_EXCHANGE, "")
        subscriber = NotificationEventBatchSubscriber(context.connectionManager, queue)
    }

    override fun subscribe(callback: MessageListener<EventBatch>, vararg attributes: String) =
        internalSubscribe(callback)

    override fun subscribeAll(callback: MessageListener<EventBatch>, vararg attributes: String) =
        internalSubscribe(callback)

    private fun internalSubscribe(
        callback: MessageListener<EventBatch>
    ): SubscriberMonitor {
        try {
            subscriber.addListener(callback)
            subscriber.start()
        } catch (e: Exception) {
            val errorMessage = "Listener can't be subscribed via the queue $queue"
            LOGGER.error(e) { errorMessage }
            throw RouterException(errorMessage)
        }
        return SubscriberMonitor { }
    }

    override fun send(message: EventBatch, vararg attributes: String) {
        internalSend(message)
    }

    override fun sendAll(message: EventBatch, vararg attributes: String) {
        internalSend(message)
    }

    private fun internalSend(message: EventBatch) {
        try {
            sender.send(message)
        } catch (e: Exception) {
            val errorMessage = "Notification cannot be send through the queue $queue"
            LOGGER.error(e) { errorMessage }
            throw RouterException(errorMessage)
        }
    }

    override fun close() {
        subscriber.close()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}
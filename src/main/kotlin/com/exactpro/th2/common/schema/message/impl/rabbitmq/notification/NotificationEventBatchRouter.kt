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
import com.exactpro.th2.common.schema.exception.RouterException
import com.exactpro.th2.common.schema.message.ConfirmationListener
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouterContext
import com.exactpro.th2.common.schema.message.NotificationRouter
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import io.github.oshai.kotlinlogging.KotlinLogging

const val NOTIFICATION_QUEUE_PREFIX = "global-notification-queue"

class NotificationEventBatchRouter : NotificationRouter<EventBatch> {
    private lateinit var queue: String
    private lateinit var sender: NotificationEventBatchSender
    private lateinit var subscriber: NotificationEventBatchSubscriber

    override fun init(context: MessageRouterContext) {
        sender = NotificationEventBatchSender(
            context.publishConnectionManager,
            context.configuration.globalNotification.exchange
        )
        queue = context.consumeConnectionManager.queueExclusiveDeclareAndBind(
            context.configuration.globalNotification.exchange
        )
        subscriber = NotificationEventBatchSubscriber(context.consumeConnectionManager, queue)
    }

    override fun send(message: EventBatch) {
        try {
            sender.send(message)
        } catch (e: Exception) {
            val errorMessage = "Notification cannot be send through the queue $queue"
            LOGGER.error(e) { errorMessage }
            throw RouterException(errorMessage)
        }
    }

    override fun subscribe(callback: MessageListener<EventBatch>): SubscriberMonitor {
        try {
            subscriber.addListener(ConfirmationListener.wrap(callback))
            subscriber.start()
        } catch (e: Exception) {
            val errorMessage = "Listener can't be subscribed via the queue $queue"
            LOGGER.error(e) { errorMessage }
            throw RouterException(errorMessage)
        }
        return SubscriberMonitor { }
    }

    override fun close() {
        subscriber.close()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}
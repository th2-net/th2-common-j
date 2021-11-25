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

import com.exactpro.th2.common.schema.event.EventBatchRouter
import com.exactpro.th2.common.schema.message.QueueAttribute.EVENT
import com.exactpro.th2.common.schema.message.QueueAttribute.PUBLISH
import com.exactpro.th2.common.schema.message.QueueAttribute.SUBSCRIBE
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.PinConfiguration

private const val NOTIFICATION_ATTRIBUTE = "global-notification"
private val REQUIRED_SEND_ATTRIBUTES = setOf(EVENT.toString(), PUBLISH.toString(), NOTIFICATION_ATTRIBUTE)
private val REQUIRED_SUBSCRIBE_ATTRIBUTES = setOf(EVENT.toString(), SUBSCRIBE.toString(), NOTIFICATION_ATTRIBUTE)
private const val NOTIFICATION_QUEUE = "global-notification-queue"

class NotificationEventBatchRouter : EventBatchRouter() {
    override fun getRequiredSendAttributes() = REQUIRED_SEND_ATTRIBUTES

    override fun getRequiredSubscribeAttributes() = REQUIRED_SUBSCRIBE_ATTRIBUTES

    override fun createSender(pinConfig: QueueConfiguration, pinName: String, bookName: String) =
        NotificationEventBatchSender(connectionManager, pinConfig.exchange)

    override fun createSubscriber(pinConfig: PinConfiguration, pinName: String) =
        NotificationEventBatchSubscriber(connectionManager, NOTIFICATION_QUEUE)
}
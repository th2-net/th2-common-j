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
import com.exactpro.th2.common.schema.message.MessageSender
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager.EXCLUSIVE_ROUTING_KEY
import java.io.IOException

class NotificationEventBatchSender(
    private val connectionManager: ConnectionManager,
    private val exchange: String
) : MessageSender<EventBatch> {
    @Deprecated(
        "Method is deprecated, please use constructor",
        ReplaceWith("NotificationEventBatchSender()")
    )
    override fun init(connectionManager: ConnectionManager, exchangeName: String, routingKey: String) {
        throw UnsupportedOperationException("Method is deprecated, please use constructor")
    }

    override fun send(message: EventBatch) {
        try {
            connectionManager.basicPublish(exchange, EXCLUSIVE_ROUTING_KEY, null, message.toByteArray())
        } catch (e: Exception) {
            throw IOException(
                "Can not send notification message: EventBatch: parent_event_id = ${message.parentEventId.id}",
                e
            )
        }
    }
}

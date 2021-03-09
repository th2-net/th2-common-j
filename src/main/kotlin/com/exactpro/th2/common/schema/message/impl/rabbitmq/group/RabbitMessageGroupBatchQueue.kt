/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.group

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.FilterFunction
import com.exactpro.th2.common.schema.message.MessageSender
import com.exactpro.th2.common.schema.message.MessageSubscriber
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitQueue
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager

class RabbitMessageGroupBatchQueue : AbstractRabbitQueue<MessageGroupBatch>() {
    override fun createSender(connectionManager: ConnectionManager, queueConfiguration: QueueConfiguration): MessageSender<MessageGroupBatch> {
        return RabbitMessageGroupBatchSender().apply {
            init(connectionManager, queueConfiguration.exchange, queueConfiguration.routingKey)
        }
    }

    override fun createSubscriber(connectionManager: ConnectionManager, queueConfiguration: QueueConfiguration, filterFunction: FilterFunction): MessageSubscriber<MessageGroupBatch> {
        return RabbitMessageGroupBatchSubscriber(queueConfiguration.filters).apply {
            init(connectionManager, SubscribeTarget(queueConfiguration.queue, queueConfiguration.routingKey, queueConfiguration.exchange), filterFunction)
        }
    }
}
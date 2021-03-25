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
import com.exactpro.th2.common.schema.message.MessageRouterContext
import com.exactpro.th2.common.schema.message.MessageSender
import com.exactpro.th2.common.schema.message.MessageSubscriber
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitQueue
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import org.jetbrains.annotations.NotNull

class RabbitMessageGroupBatchQueue(messageRouterContext: MessageRouterContext, queueConfiguration: QueueConfiguration, filterFunction: FilterFunction) : AbstractRabbitQueue<MessageGroupBatch>(messageRouterContext, queueConfiguration, filterFunction){
    override fun createSender(
        context: MessageRouterContext,
        queueConfiguration: QueueConfiguration
    ): MessageSender<MessageGroupBatch> =
        RabbitMessageGroupBatchSender(context, queueConfiguration.exchange, queueConfiguration.routingKey)

    override fun createSubscriber(
        context: MessageRouterContext,
        queueConfiguration: QueueConfiguration,
        filterFunction: FilterFunction
    ): MessageSubscriber<MessageGroupBatch> =
        RabbitMessageGroupBatchSubscriber(context,
            SubscribeTarget(queueConfiguration.queue, queueConfiguration.routingKey, queueConfiguration.exchange),
            filterFunction,
            queueConfiguration.filters)
}
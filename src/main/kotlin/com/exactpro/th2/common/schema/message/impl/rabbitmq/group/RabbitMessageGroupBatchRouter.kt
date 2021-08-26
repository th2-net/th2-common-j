/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.group

import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageGroupBatch.Builder
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractFilterStrategy
import com.exactpro.th2.common.schema.filter.strategy.impl.AnyMessageFilterStrategy
import com.exactpro.th2.common.schema.message.MessageSender
import com.exactpro.th2.common.schema.message.MessageSubscriber
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget
import com.exactpro.th2.common.schema.message.impl.rabbitmq.router.AbstractRabbitBatchMessageRouter
import com.google.protobuf.Message
import com.google.protobuf.TextFormat
import io.prometheus.client.Counter
import org.jetbrains.annotations.NotNull
import java.util.stream.Collectors

class RabbitMessageGroupBatchRouter : AbstractRabbitBatchMessageRouter<MessageGroup, MessageGroupBatch, Builder>() {

    override fun getDefaultFilterStrategy(): AbstractFilterStrategy<Message> {
        return AnyMessageFilterStrategy()
    }

    override fun splitAndFilter(
        message: MessageGroupBatch,
        pinConfiguration: @NotNull QueueConfiguration
    ): @NotNull MessageGroupBatch {
        if (pinConfiguration.filters.isEmpty()) {
            return message
        }

        val builder = MessageGroupBatch.newBuilder()
        message.groupsList.forEach { group ->
            if (group.messagesList.all { filterMessage(it, pinConfiguration.filters) }) {
                builder.addGroups(group)
            }
        }
        return builder.build()
    }

    override fun getMessages(batch: MessageGroupBatch): MutableList<MessageGroup> = batch.groupsList

    override fun createBatchBuilder(): Builder = MessageGroupBatch.newBuilder()

    override fun addMessage(builder: Builder, group: MessageGroup) {
        builder.addGroups(group)
    }

    override fun build(builder: Builder): MessageGroupBatch = builder.build()

    override fun createSender(pinConfig: QueueConfiguration): MessageSender<MessageGroupBatch> {
        return RabbitMessageGroupBatchSender().apply {
            init(connectionManager, pinConfig.exchange, pinConfig.routingKey)
        }
    }

    override fun createSubscriber(pinConfig: QueueConfiguration): MessageSubscriber<MessageGroupBatch> {
        return RabbitMessageGroupBatchSubscriber(
            pinConfig.filters,
            connectionManager.configuration.messageRecursionLimit
        ).apply {
            init(
                connectionManager,
                SubscribeTarget(
                    pinConfig.queue,
                    pinConfig.routingKey,
                    pinConfig.exchange
                )
            ) { msg: Message, filters: List<RouterFilter> -> filterMessage(msg, filters) }
        }
    }

    override fun MessageGroupBatch.toErrorString(): String {
        return TextFormat.shortDebugString(this)
    }

    override fun getDeliveryCounter(): Counter {
        return OUTGOING_MSG_GROUP_BATCH_QUANTITY
    }

    override fun getContentCounter(): Counter {
        return OUTGOING_MSG_GROUP_QUANTITY
    }

    override fun extractCountFrom(batch: MessageGroupBatch): Int {
        return batch.groupsCount
    }

    companion object {
        private val OUTGOING_MSG_GROUP_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_msg_group_batch_quantity", "Quantity of outgoing message group batches").register()
        private val OUTGOING_MSG_GROUP_QUANTITY = Counter.build("th2_mq_outgoing_msg_group_quantity", "Quantity of outgoing message groups").register()
    }
}
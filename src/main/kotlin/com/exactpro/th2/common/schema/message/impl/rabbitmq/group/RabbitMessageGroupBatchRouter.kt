/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.metrics.*
import com.exactpro.th2.common.schema.filter.strategy.impl.AnyMessageFilterStrategy
import com.exactpro.th2.common.schema.message.ConfirmationListener
import com.exactpro.th2.common.schema.message.MessageSender
import com.exactpro.th2.common.schema.message.MessageSubscriber
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.BookName
import com.exactpro.th2.common.schema.message.impl.rabbitmq.PinConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.PinName
import com.exactpro.th2.common.schema.message.toBuilderWithMetadata
import com.google.protobuf.Message
import com.google.protobuf.TextFormat
import io.prometheus.client.Counter
import org.jetbrains.annotations.NotNull

class RabbitMessageGroupBatchRouter : AbstractRabbitRouter<MessageGroupBatch>() {

    override fun splitAndFilter(
        message: MessageGroupBatch,
        pinConfiguration: @NotNull QueueConfiguration,
        pinName: PinName
    ): MessageGroupBatch? {
        if (pinConfiguration.filters.isEmpty()) {
            return message
        }

        val builder = message.toBuilderWithMetadata()
        message.groupsList.forEach { group ->
            if (group.messagesList.all { AnyMessageFilterStrategy.verify(it, pinConfiguration.filters) }) {
                builder.addGroups(group)
            } else {
                incrementDroppedMetrics(
                    group.messagesList,
                    pinName,
                    MESSAGE_DROPPED_PUBLISH_TOTAL,
                    MESSAGE_GROUP_DROPPED_PUBLISH_TOTAL
                )
            }
        }
        // should not return an empty batch
        return if (builder.groupsCount > 0) builder.build() else null
    }

    override fun createSender(
        pinConfig: QueueConfiguration,
        pinName: PinName,
        bookName: BookName
    ): MessageSender<MessageGroupBatch> {
        return RabbitMessageGroupBatchSender(
            publishConnectionManager,
            pinConfig.exchange,
            pinConfig.routingKey,
            pinName,
            bookName
        )
    }

    override fun createSubscriber(
        pinConfig: PinConfiguration,
        pinName: PinName,
        listener: ConfirmationListener<MessageGroupBatch>
    ): MessageSubscriber {
        return RabbitMessageGroupBatchSubscriber(
            consumeConnectionManager,
            pinConfig.queue,
            { msg: Message, filters: List<RouterFilter> -> AnyMessageFilterStrategy.verify(msg, filters) },
            pinName,
            pinConfig.filters,
            consumeConnectionManager.configuration.messageRecursionLimit,
            listener
        )
    }

    override fun MessageGroupBatch.toErrorString(): String {
        return TextFormat.shortDebugString(this)
    }

    companion object {
        const val MESSAGE_GROUP_TYPE = "MESSAGE_GROUP"

        private val MESSAGE_DROPPED_PUBLISH_TOTAL: Counter = Counter.build()
            .name("th2_message_dropped_publish_total")
            .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL, MESSAGE_TYPE_LABEL)
            .help("Quantity of published raw or parsed messages dropped after filters")
            .register()

        private val MESSAGE_GROUP_DROPPED_PUBLISH_TOTAL: Counter = Counter.build()
            .name("th2_message_group_dropped_publish_total")
            .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL)
            .help("Quantity of published message groups dropped after filters")
            .register()
    }
}
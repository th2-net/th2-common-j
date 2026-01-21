/*
 * Copyright 2023-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.transport

import com.exactpro.th2.common.schema.message.ConfirmationListener
import com.exactpro.th2.common.schema.message.MessageSender
import com.exactpro.th2.common.schema.message.MessageSubscriber
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.BookName
import com.exactpro.th2.common.schema.message.impl.rabbitmq.PinConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.PinName
import org.jetbrains.annotations.NotNull

class TransportGroupBatchRouter : AbstractRabbitRouter<GroupBatch>() {
    override fun splitAndFilter(
        message: GroupBatch,
        pinConfiguration: @NotNull QueueConfiguration,
        pinName: PinName,
    ): GroupBatch? = pinConfiguration.filters.filter(message)

    override fun createSender(
        pinConfig: QueueConfiguration,
        pinName: PinName,
        bookName: BookName,
    ): MessageSender<GroupBatch> {
        return TransportGroupBatchSender(
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
        listener: ConfirmationListener<GroupBatch>
    ): MessageSubscriber {
        return TransportGroupBatchSubscriber(
            consumeConnectionManager,
            pinConfig.queue,
            pinName,
            pinConfig.filters,
            listener
        )
    }

    override fun GroupBatch.toErrorString(): String = toString()

    override fun getRequiredSendAttributes(): Set<String> = REQUIRED_SEND_ATTRIBUTES
    override fun getRequiredSubscribeAttributes(): Set<String> = REQUIRED_SUBSCRIBE_ATTRIBUTES

    companion object {
        const val TRANSPORT_GROUP_TYPE = "TRANSPORT_GROUP"
        const val TRANSPORT_GROUP_ATTRIBUTE = "transport-group"
        private val REQUIRED_SUBSCRIBE_ATTRIBUTES = setOf(QueueAttribute.SUBSCRIBE.value, TRANSPORT_GROUP_ATTRIBUTE)
        private val REQUIRED_SEND_ATTRIBUTES = setOf(QueueAttribute.PUBLISH.value, TRANSPORT_GROUP_ATTRIBUTE)
    }
}
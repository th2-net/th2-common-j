/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.demo

import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractFilterStrategy
import com.exactpro.th2.common.schema.filter.strategy.impl.AnyMessageFilterStrategy
import com.exactpro.th2.common.schema.message.MessageSender
import com.exactpro.th2.common.schema.message.MessageSubscriber
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.BookName
import com.exactpro.th2.common.schema.message.impl.rabbitmq.PinName
import com.google.protobuf.Message
import org.jetbrains.annotations.NotNull

class DemoMessageBatchRouter : AbstractRabbitRouter<DemoMessageBatch>() {
    override fun getDefaultFilterStrategy(): AbstractFilterStrategy<Message> {
        return AnyMessageFilterStrategy()
    }

    override fun splitAndFilter(
        message: DemoMessageBatch,
        pinConfiguration: @NotNull QueueConfiguration,
        pinName: PinName
    ): DemoMessageBatch {
        //TODO: Implement - whole batch or null
        return message
    }

    override fun createSender(
        pinConfig: QueueConfiguration,
        pinName: PinName,
        bookName: BookName
    ): MessageSender<DemoMessageBatch> {
        return DemoMessageBatchSender(
            connectionManager,
            pinConfig.exchange,
            pinConfig.routingKey,
            pinName,
            bookName
        )
    }

    override fun createSubscriber(
        pinConfig: QueueConfiguration,
        pinName: PinName
    ): MessageSubscriber<DemoMessageBatch> {
        return DemoMessageBatchSubscriber(
            connectionManager,
            pinConfig.queue,
            pinName
        )
    }

    override fun DemoMessageBatch.toErrorString(): String = toString()

    override fun getRequiredSendAttributes(): Set<String> = REQUIRED_SEND_ATTRIBUTES
    override fun getRequiredSubscribeAttributes(): Set<String> = REQUIRED_SUBSCRIBE_ATTRIBUTES

    companion object {
        internal const val DEMO_RAW_MESSAGE_TYPE = "DEMO_RAW_MESSAGE"
        internal const val DEMO_RAW_ATTRIBUTE = "demo_raw"
        private val REQUIRED_SUBSCRIBE_ATTRIBUTES = setOf(DEMO_RAW_ATTRIBUTE, QueueAttribute.SUBSCRIBE.value)
        private val REQUIRED_SEND_ATTRIBUTES = setOf(DEMO_RAW_ATTRIBUTE, QueueAttribute.PUBLISH.value)
    }
}
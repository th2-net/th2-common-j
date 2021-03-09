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

import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageGroupBatch.Builder
import com.exactpro.th2.common.schema.filter.strategy.impl.DefaultFilterStrategy
import com.exactpro.th2.common.schema.message.FilterFunction
import com.exactpro.th2.common.schema.message.MessageQueue
import com.exactpro.th2.common.schema.message.QueueAttribute.PUBLISH
import com.exactpro.th2.common.schema.message.QueueAttribute.SUBSCRIBE
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.exactpro.th2.common.schema.message.impl.rabbitmq.router.AbstractRabbitBatchMessageRouter
import com.exactpro.th2.common.schema.message.toJson
import com.exactpro.th2.common.schema.strategy.fieldExtraction.impl.AnyMessageFieldExtractionStrategy

class RabbitMessageGroupBatchRouter : AbstractRabbitBatchMessageRouter<MessageGroup, MessageGroupBatch, Builder>() {

    override fun getDefaultFilterStrategy(): DefaultFilterStrategy {
        return DefaultFilterStrategy(AnyMessageFieldExtractionStrategy())
    }

    override fun createQueue(connectionManager: ConnectionManager, queueConfiguration: QueueConfiguration, filterFunction: FilterFunction): MessageQueue<MessageGroupBatch> {
        return RabbitMessageGroupBatchQueue().apply {
            init(connectionManager, queueConfiguration, filterFunction)
        }
    }

    override fun findQueueByFilter(queues: MutableMap<String, QueueConfiguration>, batch: MessageGroupBatch): MutableMap<String, MessageGroupBatch> {
        val builders = hashMapOf<String, Builder>()

        getMessages(batch).forEach { group ->
            val originalMessages = group.messagesList
            val parsedRawPartition = originalMessages.partition { it.hasMessage() }

            if (parsedRawPartition.first.size == originalMessages.size || parsedRawPartition.second.size == originalMessages.size) {
                val forFilter = if (parsedRawPartition.first.isEmpty()) parsedRawPartition.second else parsedRawPartition.first
                forFilter.groupBy { filter(queues, it) }
                    .flatMap { entry -> entry.key.map { it to entry.value } }
                    .toMap()
                    .forEach {
                        builders
                            .getOrPut(it.key, ::createBatchBuilder)
                            .addGroups(
                                if (it.value.size == group.messagesCount)
                                    group
                                else
                                    MessageGroup.newBuilder().addAllMessages(it.value).build()
                            )
                    }
            } else {
                var skipped = false;
                originalMessages.groupBy { filter(queues, it) }.forEach { (aliases, messages) ->
                    if (aliases.isNotEmpty() && messages.size == group.messagesCount) {
                        aliases.forEach { alias ->
                            builders.getOrPut(alias, ::createBatchBuilder).addGroups(group)
                        }
                    } else {
                        skipped = true
                    }
                }

                if (skipped) {
                    monitor.onWarn("Group was skipped for some queues = " + group.toJson())
                }
            }
        }

        return builders.mapValuesTo(hashMapOf()) { it.value.build() }
    }

    override fun requiredSubscribeAttributes(): Set<String> = REQUIRED_SUBSCRIBE_ATTRIBUTES

    override fun requiredSendAttributes(): Set<String> = REQUIRED_SEND_ATTRIBUTES

    override fun getMessages(batch: MessageGroupBatch): MutableList<MessageGroup> = batch.groupsList

    override fun createBatchBuilder(): Builder = MessageGroupBatch.newBuilder()

    override fun addMessage(builder: Builder, group: MessageGroup) {
        builder.addGroups(group)
    }

    override fun build(builder: Builder): MessageGroupBatch = builder.build()

    companion object {
        private val REQUIRED_SUBSCRIBE_ATTRIBUTES = setOf(SUBSCRIBE.toString())
        private val REQUIRED_SEND_ATTRIBUTES = setOf(PUBLISH.toString())
    }
}
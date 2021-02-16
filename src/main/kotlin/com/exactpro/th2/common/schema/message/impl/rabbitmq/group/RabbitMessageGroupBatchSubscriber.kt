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

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.filter.strategy.FilterStrategy
import com.exactpro.th2.common.schema.filter.strategy.impl.DefaultFilterStrategy
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitBatchSubscriber
import com.exactpro.th2.common.schema.message.toJson
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import mu.KotlinLogging

class RabbitMessageGroupBatchSubscriber(
    private val filters: List<RouterFilter>,
    private val filterStrategy: FilterStrategy = DefaultFilterStrategy()
) : AbstractRabbitBatchSubscriber<MessageGroup, MessageGroupBatch>(filters, filterStrategy) {
    private val logger = KotlinLogging.logger {}

    override fun getDeliveryCounter(): Counter = INCOMING_MSG_GROUP_BATCH_QUANTITY
    override fun getContentCounter(): Counter = INCOMING_MSG_GROUP_QUANTITY
    override fun getProcessingTimer(): Gauge = MSG_GROUP_PROCESSING_TIME
    override fun extractCountFrom(message: MessageGroupBatch): Int = message.groupsCount
    override fun valueFromBytes(body: ByteArray): List<MessageGroupBatch> = listOf(MessageGroupBatch.parseFrom(body))
    override fun getMessages(batch: MessageGroupBatch): MutableList<MessageGroup> = batch.groupsList
    override fun createBatch(messages: List<MessageGroup>): MessageGroupBatch = MessageGroupBatch.newBuilder().addAllGroups(messages).build()
    override fun toShortDebugString(value: MessageGroupBatch): String = value.toJson()

    override fun extractMetadata(messageGroup: MessageGroup): Metadata = throw UnsupportedOperationException()

    private fun extractMetadata(message: AnyMessage): Metadata {
        val (messageType, messageId) = when (val kindCase = message.kindCase) {
            MESSAGE -> message.message.metadata.run { messageType to id }
            RAW_MESSAGE -> message.rawMessage.metadata.run { RAW_MESSAGE_TYPE to id }
            else -> error("Unsupported group message kind: $kindCase")
        }

        return Metadata(
            messageId.sequence,
            messageType,
            messageId.direction,
            messageId.connectionId.sessionAlias
        )
    }

    override fun filter(batch: MessageGroupBatch): MessageGroupBatch? {
        if (filters.isEmpty()) {
            return batch
        }

        val groups = getMessages(batch).asSequence()
            .map { group ->
                group.messagesList.filter { message ->
                    if (!filterStrategy.verify(message, filters)) {
                        logger.debug { "Skipped message because it didn't match any filters: ${extractMetadata(message)}" }
                        false
                    } else {
                        true
                    }
                }
            }
            .filter { it.isNotEmpty() }
            .map { MessageGroup.newBuilder().addAllMessages(it).build() }
            .toList()

        return if (groups.isEmpty()) null else createBatch(groups)
    }

    companion object {
        private const val RAW_MESSAGE_TYPE = "raw"
        private val INCOMING_MSG_GROUP_BATCH_QUANTITY = Counter.build("th2_mq_incoming_msg_group_batch_quantity", "Quantity of incoming message group batches").register()
        private val INCOMING_MSG_GROUP_QUANTITY = Counter.build("th2_mq_incoming_msg_group_quantity", "Quantity of incoming message groups").register()
        private val MSG_GROUP_PROCESSING_TIME = Gauge.build("th2_mq_msg_group_processing_time", "Time of processing message groups").register()
    }
}
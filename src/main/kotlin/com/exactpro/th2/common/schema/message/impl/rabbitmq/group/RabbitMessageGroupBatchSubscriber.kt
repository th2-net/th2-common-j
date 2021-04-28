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
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.metrics.DEFAULT_BUCKETS
import com.exactpro.th2.common.metrics.DEFAULT_DIRECTION_LABEL_NAME
import com.exactpro.th2.common.metrics.DEFAULT_SESSION_ALIAS_LABEL_NAME
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitBatchSubscriber
import io.prometheus.client.Counter
import io.prometheus.client.Histogram
import mu.KotlinLogging

class RabbitMessageGroupBatchSubscriber(
    private val filters: List<RouterFilter>
) : AbstractRabbitBatchSubscriber<MessageGroup, MessageGroupBatch>(filters) {
    private val logger = KotlinLogging.logger {}

    override fun getDeliveryCounter(): Counter = INCOMING_MSG_GROUP_BATCH_QUANTITY
    override fun getContentCounter(): Counter = INCOMING_MSG_GROUP_QUANTITY
    override fun getProcessingTimer(): Histogram = MSG_GROUP_PROCESSING_TIME
    override fun extractLabels(batch: MessageGroupBatch): Array<String> {
        val message = getMessages(batch)[0].messagesList[0]

        return when {
            message.hasMessage() -> {
                val messageID = message.message.metadata.id
                arrayOf(messageID.connectionId.sessionAlias, messageID.direction.name)
            }
            message.hasRawMessage() -> {
                val messageID = message.rawMessage.metadata.id
                arrayOf(messageID.connectionId.sessionAlias, messageID.direction.name)
            }
            else -> {
                emptyArray()
            }
        }
    }
    override fun extractCountFrom(batch: MessageGroupBatch): Int = batch.groupsCount
    override fun valueFromBytes(body: ByteArray): List<MessageGroupBatch> = listOf(MessageGroupBatch.parseFrom(body))
    override fun getMessages(batch: MessageGroupBatch): MutableList<MessageGroup> = batch.groupsList
    override fun createBatch(messages: List<MessageGroup>): MessageGroupBatch = MessageGroupBatch.newBuilder().addAllGroups(messages).build()
    override fun toShortDebugString(value: MessageGroupBatch): String = value.toJson()

    override fun extractMetadata(messageGroup: MessageGroup): Metadata = throw UnsupportedOperationException()

    override fun filter(batch: MessageGroupBatch): MessageGroupBatch? {
        if (filters.isEmpty()) {
            return batch
        }

        val groups = getMessages(batch).asSequence()
            .filter { group ->
                group.messagesList.all { message ->
                    callFilterFunction(message, filters)
                }.also { allMessagesMatch ->
                    if (!allMessagesMatch) {
                        logger.debug { "Skipped message group because none or some of its messages didn't match any filters: ${group.toJson()}" }
                    }
                }
            }
            .toList()

        return if (groups.isEmpty()) null else createBatch(groups)
    }

    companion object {
        private val INCOMING_MSG_GROUP_BATCH_QUANTITY = Counter.build()
            .name("th2_mq_incoming_msg_group_batch_quantity")
            .labelNames(DEFAULT_SESSION_ALIAS_LABEL_NAME, DEFAULT_DIRECTION_LABEL_NAME)
            .help("Quantity of incoming message group batches")
            .register()
        private val INCOMING_MSG_GROUP_QUANTITY = Counter.build()
            .name("th2_mq_incoming_msg_group_quantity")
            .labelNames(DEFAULT_SESSION_ALIAS_LABEL_NAME, DEFAULT_DIRECTION_LABEL_NAME)
            .help("Quantity of incoming message groups")
            .register()
        private val MSG_GROUP_PROCESSING_TIME = Histogram.build("th2_mq_msg_group_processing_time", "Time of processing message groups").buckets(*DEFAULT_BUCKETS).register()
    }
}
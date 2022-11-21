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
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.metrics.DIRECTION_LABEL
import com.exactpro.th2.common.metrics.MESSAGE_TYPE_LABEL
import com.exactpro.th2.common.metrics.SESSION_ALIAS_LABEL
import com.exactpro.th2.common.metrics.TH2_PIN_LABEL
import com.exactpro.th2.common.metrics.incrementDroppedMetrics
import com.exactpro.th2.common.metrics.incrementTotalMetrics
import com.exactpro.th2.common.schema.message.FilterFunction
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSubscriber
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback
import com.exactpro.th2.common.schema.message.toBuilderWithMetadata
import com.exactpro.th2.common.schema.message.impl.rabbitmq.group.RabbitMessageGroupBatchRouter.Companion.MESSAGE_GROUP_TYPE
import com.exactpro.th2.common.schema.message.toShortDebugString
import com.google.protobuf.CodedInputStream
import com.rabbitmq.client.Delivery
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import mu.KotlinLogging

class RabbitMessageGroupBatchSubscriber(
    connectionManager: ConnectionManager,
    queue: String,
    filterFunction: FilterFunction,
    th2Pin: String,
    private val filters: List<RouterFilter>,
    private val messageRecursionLimit: Int
) : AbstractRabbitSubscriber<MessageGroupBatch>(connectionManager, queue, filterFunction, th2Pin, MESSAGE_GROUP_TYPE) {
    private val logger = KotlinLogging.logger {}

    override fun valueFromBytes(body: ByteArray): MessageGroupBatch = parseEncodedBatch(body)

    override fun toShortTraceString(value: MessageGroupBatch): String = value.toJson()

    override fun toShortDebugString(value: MessageGroupBatch): String = value.toShortDebugString()

    override fun filter(batch: MessageGroupBatch): MessageGroupBatch? {
        if (filters.isEmpty()) {
            return batch
        }

        val groups = batch.groupsList.asSequence()
            .filter { group ->
                group.messagesList.all { message ->
                    callFilterFunction(message, filters)
                }.also { allMessagesMatch ->
                    if (!allMessagesMatch) {
                        logger.debug { "Skipped message group because none or some of its messages didn't match any filters: ${group.toJson()}" }
                        incrementDroppedMetrics(
                            group.messagesList,
                            th2Pin,
                            MESSAGE_DROPPED_SUBSCRIBE_TOTAL,
                            MESSAGE_GROUP_DROPPED_SUBSCRIBE_TOTAL
                        )
                    }
                }
            }
            .toList()

        return if (groups.isEmpty()) null else batch.toBuilderWithMetadata().addAllGroups(groups).build()
    }

    override fun handle(
        consumeTag: String,
        delivery: Delivery,
        value: MessageGroupBatch,
        confirmation: ManualAckDeliveryCallback.Confirmation
    ) {
        incrementTotalMetrics(
            value,
            th2Pin,
            MESSAGE_SUBSCRIBE_TOTAL,
            MESSAGE_GROUP_SUBSCRIBE_TOTAL,
            MESSAGE_GROUP_SEQUENCE_SUBSCRIBE
        )
        super.handle(consumeTag, delivery, value, confirmation)
    }

    private fun parseEncodedBatch(body: ByteArray?): MessageGroupBatch {
        val ins = CodedInputStream.newInstance(body)
        ins.setRecursionLimit(messageRecursionLimit)
        return MessageGroupBatch.parseFrom(ins)
    }

    companion object {
        private val MESSAGE_SUBSCRIBE_TOTAL = Counter.build()
            .name("th2_message_subscribe_total")
            .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL, MESSAGE_TYPE_LABEL)
            .help("Quantity of received raw or parsed messages, includes dropped after filters. " +
                    "For information about the number of dropped messages, please refer to 'th2_message_dropped_subscribe_total'")
            .register()

        private val MESSAGE_GROUP_SUBSCRIBE_TOTAL = Counter.build()
            .name("th2_message_group_subscribe_total")
            .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL)
            .help("Quantity of received message groups, includes dropped after filters. " +
                    "For information about the number of dropped messages, please refer to 'th2_message_group_dropped_subscribe_total'")
            .register()

        private val MESSAGE_DROPPED_SUBSCRIBE_TOTAL = Counter.build()
            .name("th2_message_dropped_subscribe_total")
            .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL, MESSAGE_TYPE_LABEL)
            .help("Quantity of received raw or parsed messages dropped after filters")
            .register()

        private val MESSAGE_GROUP_DROPPED_SUBSCRIBE_TOTAL = Counter.build()
            .name("th2_message_group_dropped_subscribe_total")
            .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL)
            .help("Quantity of received message groups dropped after filters")
            .register()

        private val MESSAGE_GROUP_SEQUENCE_SUBSCRIBE = Gauge.build()
            .name("th2_message_group_sequence_subscribe")
            .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL)
            .help("Last received sequence")
            .register()
    }
}
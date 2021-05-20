/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.group

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.getSessionAliasAndDirection
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender
import io.prometheus.client.Counter

class RabbitMessageGroupBatchSender : AbstractRabbitSender<MessageGroupBatch>() {
    override fun getDeliveryCounter(): Counter = OUTGOING_MSG_GROUP_BATCH_QUANTITY
    override fun getContentCounter(): Counter = OUTGOING_MSG_GROUP_QUANTITY
    override fun extractCountFrom(batch: MessageGroupBatch): Int = batch.groupsCount
    override fun valueToBytes(value: MessageGroupBatch): ByteArray = value.toByteArray()
    override fun toShortTraceString(value: MessageGroupBatch): String = value.toJson()
    override fun toShortDebugString(value: MessageGroupBatch): String = "MessageGroupBatch: " +
        run {
            val sessionAliasAndDirection = getSessionAliasAndDirection(value.groupsList[0].messagesList[0])
            "session alias = ${sessionAliasAndDirection[0]}, direction = ${sessionAliasAndDirection[1]}"
        } +
        value.groupsList.flatMap { it.messagesList }.joinToString(prefix = ", sequences = ") {
            when {
                it.hasMessage() -> it.message.metadata.id.sequence.toString()
                it.hasRawMessage() -> it.rawMessage.metadata.id.sequence.toString()
                else -> ""
            }
        }

    companion object {
        private val OUTGOING_MSG_GROUP_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_msg_group_batch_quantity", "Quantity of outgoing message group batches").register()
        private val OUTGOING_MSG_GROUP_QUANTITY = Counter.build("th2_mq_outgoing_msg_group_quantity", "Quantity of outgoing message groups").register()
    }
}
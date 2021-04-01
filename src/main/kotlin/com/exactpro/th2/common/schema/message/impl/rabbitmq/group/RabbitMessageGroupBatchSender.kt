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
import com.exactpro.th2.common.schema.message.MessageRouterContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.exactpro.th2.common.schema.message.toJson
import io.prometheus.client.Counter
import org.jetbrains.annotations.NotNull

class RabbitMessageGroupBatchSender(
    messageRouterContext: MessageRouterContext, exchangeName: String,
    sendQueue: String
) : AbstractRabbitSender<MessageGroupBatch>(messageRouterContext, exchangeName, sendQueue) {
    override fun getDeliveryCounter(): Counter = OUTGOING_MSG_GROUP_BATCH_QUANTITY
    override fun getContentCounter(): Counter = OUTGOING_MSG_GROUP_QUANTITY
    override fun extractCountFrom(message: MessageGroupBatch): Int = message.groupsCount
    override fun valueToBytes(value: MessageGroupBatch): ByteArray = value.toByteArray()
    override fun toShortDebugString(value: MessageGroupBatch): String = value.toJson()

    companion object {
        private val OUTGOING_MSG_GROUP_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_msg_group_batch_quantity", "Quantity of outgoing message group batches").register()
        private val OUTGOING_MSG_GROUP_QUANTITY = Counter.build("th2_mq_outgoing_msg_group_quantity", "Quantity of outgoing message groups").register()
    }
}
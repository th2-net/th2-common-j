/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.metrics.DIRECTION_LABEL
import com.exactpro.th2.common.metrics.SESSION_ALIAS_LABEL
import com.exactpro.th2.common.metrics.TH2_PIN_LABEL
import com.exactpro.th2.common.metrics.MESSAGE_TYPE_LABEL
import com.exactpro.th2.common.metrics.incrementTotalMetrics
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender
import com.exactpro.th2.common.schema.message.impl.rabbitmq.PinConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.exactpro.th2.common.schema.message.impl.rabbitmq.group.RabbitMessageGroupBatchRouter.Companion.MESSAGE_GROUP_TYPE
import io.prometheus.client.Counter
import io.prometheus.client.Gauge

class RabbitMessageGroupBatchSender(
    connectionManager: ConnectionManager,
    pinConfig: PinConfiguration,
    th2Pin: String
) : AbstractRabbitSender<MessageGroupBatch>(connectionManager, pinConfig, th2Pin, MESSAGE_GROUP_TYPE) {
    override fun send(value: MessageGroupBatch) {
        incrementTotalMetrics(
            value,
            th2Pin,
            MESSAGE_PUBLISH_TOTAL,
            MESSAGE_GROUP_PUBLISH_TOTAL,
            MESSAGE_GROUP_SEQUENCE_PUBLISH
        )
        super.send(value)
    }

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
        private val MESSAGE_PUBLISH_TOTAL = Counter.build()
            .name("th2_message_publish_total")
            .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL, MESSAGE_TYPE_LABEL)
            .help("Quantity of published raw or parsed messages")
            .register()

        private val MESSAGE_GROUP_PUBLISH_TOTAL = Counter.build()
            .name("th2_message_group_publish_total")
            .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL)
            .help("Quantity of published message groups")
            .register()

        private val MESSAGE_GROUP_SEQUENCE_PUBLISH = Gauge.build()
            .name("th2_message_group_sequence_publish")
            .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL)
            .help("Last published sequence")
            .register()
    }
}
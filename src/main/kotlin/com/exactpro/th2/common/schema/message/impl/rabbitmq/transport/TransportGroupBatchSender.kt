/*
 *  Copyright 2025 Exactpro (Exactpro Systems Limited)
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.common.schema.message.impl.rabbitmq.transport

import com.exactpro.th2.common.metrics.BOOK_NAME_LABEL
import com.exactpro.th2.common.metrics.SESSION_GROUP_LABEL
import com.exactpro.th2.common.metrics.TH2_PIN_LABEL
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender
import com.exactpro.th2.common.schema.message.impl.rabbitmq.BookName
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.PublishConnectionManager
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.TransportGroupBatchRouter.Companion.TRANSPORT_GROUP_TYPE
import io.prometheus.client.Counter

class TransportGroupBatchSender(
    publishConnectionManager: PublishConnectionManager,
    exchangeName: String,
    routingKey: String,
    th2Pin: String,
    bookName: BookName,
) : AbstractRabbitSender<GroupBatch>(
    publishConnectionManager,
    exchangeName,
    routingKey,
    th2Pin,
    TRANSPORT_GROUP_TYPE,
    bookName
) {
    override fun send(value: GroupBatch) {
        TRANSPORT_GROUP_PUBLISH_TOTAL
            .labels(th2Pin, value.book, value.sessionGroup)
            .inc(value.groups.size.toDouble())

        super.send(value)
    }

    override fun valueToBytes(value: GroupBatch): ByteArray = value.toByteArray()

    override fun toShortTraceString(value: GroupBatch): String = value.toString()

    override fun toShortDebugString(value: GroupBatch): String = value.toString()

    companion object {
        private val TRANSPORT_GROUP_PUBLISH_TOTAL = Counter.build()
            .name("th2_transport_group_publish_total")
            .labelNames(TH2_PIN_LABEL, BOOK_NAME_LABEL, SESSION_GROUP_LABEL)
            .help("Quantity of published transport groups")
            .withoutExemplars()
            .register()
    }
}
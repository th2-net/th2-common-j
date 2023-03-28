/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.demo

import com.exactpro.th2.common.metrics.BOOK_NAME_LABEL
import com.exactpro.th2.common.metrics.SESSION_GROUP_LABEL
import com.exactpro.th2.common.metrics.TH2_PIN_LABEL
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSubscriber
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageBatchRouter.Companion.DEMO_RAW_MESSAGE_TYPE
import com.rabbitmq.client.Delivery
import io.netty.buffer.Unpooled
import io.prometheus.client.Counter

class DemoMessageBatchSubscriber(
    connectionManager: ConnectionManager,
    queue: String,
    th2Pin: String,
) : AbstractRabbitSubscriber<DemoGroupBatch>(connectionManager, queue, th2Pin, DEMO_RAW_MESSAGE_TYPE) {

    override fun valueFromBytes(body: ByteArray): DemoGroupBatch = Unpooled.wrappedBuffer(body).run(GroupBatchCodec::decode)

    override fun toShortTraceString(value: DemoGroupBatch): String = value.toString()

    override fun toShortDebugString(value: DemoGroupBatch): String = value.toString()

    override fun filter(batch: DemoGroupBatch): DemoGroupBatch {
        //TODO: Implement - whole batch or null
        return batch
    }

    override fun handle(
        deliveryMetadata: DeliveryMetadata,
        delivery: Delivery,
        value: DemoGroupBatch,
        confirmation: ManualAckDeliveryCallback.Confirmation,
    ) {
        DEMO_RAW_MESSAGE_SUBSCRIBE_TOTAL
            .labels(th2Pin, value.book, value.sessionGroup)
            .inc(value.groups.size.toDouble())
        super.handle(deliveryMetadata, delivery, value, confirmation)
    }

    companion object {
        private val DEMO_RAW_MESSAGE_SUBSCRIBE_TOTAL = Counter.build()
            .name("th2_demo_raw_message_subscribe_total")
            .labelNames(TH2_PIN_LABEL, BOOK_NAME_LABEL, SESSION_GROUP_LABEL)
            .help("Quantity of received demo raw messages")
            .register()
    }
}
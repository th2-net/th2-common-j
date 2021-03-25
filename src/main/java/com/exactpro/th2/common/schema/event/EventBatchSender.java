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

package com.exactpro.th2.common.schema.event;

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.message.MessageUtilsKt;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender;
import io.prometheus.client.Counter;

public class EventBatchSender extends AbstractRabbitSender<EventBatch> {

    private static final Counter OUTGOING_EVENT_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_event_batch_quantity", "Quantity of outgoing event batches").register();
    private static final Counter OUTGOING_EVENT_QUANTITY = Counter.build("th2_mq_outgoing_event_quantity", "Quantity of outgoing events").register();

    @Override
    protected Counter getDeliveryCounter() {
        return OUTGOING_EVENT_BATCH_QUANTITY;
    }

    @Override
    protected Counter getContentCounter() {
        return OUTGOING_EVENT_QUANTITY;
    }

    @Override
    protected int extractCountFrom(EventBatch message) {
        return message.getEventsCount();
    }

    @Override
    protected byte[] valueToBytes(EventBatch value) {
        return value.toByteArray();
    }

    @Override
    protected String toShortDebugString(EventBatch value) {
        return MessageUtilsKt.toJson(value);
    }
}

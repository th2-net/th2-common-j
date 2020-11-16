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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.raw;

import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender;
import com.google.protobuf.TextFormat;

import io.prometheus.client.Counter;

public class RabbitRawBatchSender extends AbstractRabbitSender<RawMessageBatch> {

    private static final Counter OUTGOING_RAW_MSG_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_raw_msg_batch_quantity", "Quantity of outgoing raw message batches").register();
    private static final Counter OUTGOING_RAW_MSG_QUANTITY = Counter.build("th2_mq_outgoing_raw_msg_quantity", "Quantity of outgoing raw messages").register();

    @Override
    protected Counter getCounter() {
        return OUTGOING_RAW_MSG_BATCH_QUANTITY;
    }

    @Override
    protected Counter getContentCounter() {
        return OUTGOING_RAW_MSG_QUANTITY;
    }

    @Override
    protected int extractCountFrom(RawMessageBatch message) {
        return message.getMessagesCount();
    }

    @Override
    protected byte[] valueToBytes(RawMessageBatch value) {
        return value.toByteArray();
    }

    @Override
    protected String toShortDebugString(RawMessageBatch value) {
        return TextFormat.shortDebugString(value);
    }
}

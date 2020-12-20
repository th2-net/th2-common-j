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

import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.filter.strategy.FilterStrategy;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitBatchSubscriber;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;

import java.util.List;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

public class RabbitRawBatchSubscriber extends AbstractRabbitBatchSubscriber<RawMessage, RawMessageBatch> {

    private static final Counter INCOMING_RAW_MSG_BATCH_QUANTITY = Counter.build("th2_mq_incoming_raw_msg_batch_quantity", "Quantity of incoming raw message batches").register();
    private static final Counter INCOMING_RAW_MSG_QUANTITY = Counter.build("th2_mq_incoming_raw_msg_quantity", "Quantity of incoming raw messages").register();
    private static final Gauge RAW_MSG_PROCESSING_TIME = Gauge.build("th2_mq_raw_msg_processing_time", "Time of processing raw messages").register();

    private static final String MESSAGE_TYPE = "raw";

    public RabbitRawBatchSubscriber(List<? extends RouterFilter> filters) {
        super(filters);
    }

    public RabbitRawBatchSubscriber(List<? extends RouterFilter> filters, FilterStrategy filterStrategy) {
        super(filters, filterStrategy);
    }

    @Override
    protected Counter getDeliveryCounter() {
        return INCOMING_RAW_MSG_BATCH_QUANTITY;
    }

    @Override
    protected Counter getContentCounter() {
        return INCOMING_RAW_MSG_QUANTITY;
    }

    @Override
    protected Gauge getProcessingTimer() {
        return RAW_MSG_PROCESSING_TIME;
    }

    @Override
    protected int extractCountFrom(RawMessageBatch message) {
        return message.getMessagesCount();
    }

    @Override
    protected RawMessageBatch valueFromBytes(byte[] body) throws InvalidProtocolBufferException {
        return RawMessageBatch.parseFrom(body);
    }

    @Override
    protected List<RawMessage> getMessages(RawMessageBatch batch) {
        return batch.getMessagesList();
    }

    @Override
    protected RawMessageBatch createBatch(List<RawMessage> messages) {
        return RawMessageBatch.newBuilder().addAllMessages(messages).build();
    }

    @Override
    protected String toShortDebugString(RawMessageBatch value) {
        return TextFormat.shortDebugString(value);
    }

    @Override
    protected Metadata extractMetadata(RawMessage message) {
        var metadata = message.getMetadata();
        var messageID = metadata.getId();
        return new Metadata(messageID.getSequence(), MESSAGE_TYPE, messageID.getConnectionId().getSessionAlias(), messageID.getDirection());
    }

}

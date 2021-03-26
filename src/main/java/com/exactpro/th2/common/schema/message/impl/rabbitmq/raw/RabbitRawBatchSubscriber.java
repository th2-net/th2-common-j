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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.raw;

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.AnyMessage.KindCase;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.message.MessageRouterUtils;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitBatchSubscriber;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.custom.MetricsHolder;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

import java.util.ArrayList;
import java.util.List;

public class RabbitRawBatchSubscriber extends AbstractRabbitBatchSubscriber<RawMessage, RawMessageBatch> {

    private static final String TAG = "raw msg";
    private static final Counter RAW_MSG_PROCESSING_FAILURE_QUANTITY = MetricsHolder.Companion.registerProcessingFailureDescribable(TAG);
    private static final Histogram RAW_MSG_BATCH_PROCESSING_TIME = MetricsHolder.Companion.registerProcessingDescribable(TAG + " batch");
    private static final Histogram RAW_MSG_PROCESSING_TIME = MetricsHolder.Companion.registerProcessingDescribable(TAG);

    private static final String MESSAGE_TYPE = "raw";

    public RabbitRawBatchSubscriber(List<? extends RouterFilter> filters) {
        super(filters);
    }

    @Override
    protected Histogram getDeliveryProcessingHistogram() {
        return RAW_MSG_BATCH_PROCESSING_TIME;
    }

    @Override
    protected Histogram getDataProcessingHistogram() {
        return RAW_MSG_PROCESSING_TIME;
    }

    @Override
    protected Counter getDataProcessingFailureCounter() {
        return RAW_MSG_PROCESSING_FAILURE_QUANTITY;
    }

    @Override
    protected int extractCountFrom(RawMessageBatch message) {
        return message.getMessagesCount();
    }

    @Override
    protected List<RawMessageBatch> valueFromBytes(byte[] body) throws Exception {
        var groupBatch = MessageGroupBatch.parseFrom(body);
        var messageGroups = groupBatch.getGroupsList();
        var rawBatches = new ArrayList<RawMessageBatch>(messageGroups.size());

        for (var group : messageGroups) {
            var builder = RawMessageBatch.newBuilder();

            for (AnyMessage message : group.getMessagesList()) {
                if (message.getKindCase() != KindCase.RAW_MESSAGE) {
                    throw new IllegalStateException("Message group batch contains parsed messages: " + MessageRouterUtils.toJson(groupBatch));
                }

                builder.addMessages(message.getRawMessage());
            }

            rawBatches.add(builder.build());
        }

        return rawBatches;
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
        return MessageRouterUtils.toJson(value);
    }

    @Override
    protected Metadata extractMetadata(RawMessage message) {
        var metadata = message.getMetadata();
        var messageID = metadata.getId();
        return new Metadata(
                messageID.getSequence(),
                MESSAGE_TYPE,
                messageID.getDirection(),
                messageID.getConnectionId().getSessionAlias()
        );
    }

}

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

import static com.exactpro.th2.common.message.MessageUtils.getDebugString;
import static com.exactpro.th2.common.message.MessageUtils.getSessionAliasAndDirection;
import static com.exactpro.th2.common.metrics.CommonMetrics.DEFAULT_BUCKETS;
import static com.exactpro.th2.common.metrics.CommonMetrics.DEFAULT_DIRECTION_LABEL_NAME;
import static com.exactpro.th2.common.metrics.CommonMetrics.DEFAULT_SESSION_ALIAS_LABEL_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.AnyMessage.KindCase;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitBatchSubscriber;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

public class RabbitRawBatchSubscriber extends AbstractRabbitBatchSubscriber<RawMessage, RawMessageBatch> {

    private static final Counter INCOMING_RAW_MSG_BATCH_QUANTITY = Counter.build()
            .name("th2_mq_incoming_raw_msg_batch_quantity")
            .labelNames(DEFAULT_SESSION_ALIAS_LABEL_NAME, DEFAULT_DIRECTION_LABEL_NAME)
            .help("Quantity of incoming raw message batches")
            .register();
    private static final Counter INCOMING_RAW_MSG_QUANTITY = Counter.build()
            .name("th2_mq_incoming_raw_msg_quantity")
            .labelNames(DEFAULT_SESSION_ALIAS_LABEL_NAME, DEFAULT_DIRECTION_LABEL_NAME)
            .help("Quantity of incoming raw messages")
            .register();
    private static final Histogram RAW_MSG_PROCESSING_TIME = Histogram.build()
            .buckets(DEFAULT_BUCKETS)
            .name("th2_mq_raw_msg_processing_time")
            .help("Time of processing raw messages")
            .register();

    private static final String MESSAGE_TYPE = "raw";

    public RabbitRawBatchSubscriber(List<? extends RouterFilter> filters, int messageRecursionLimit) {
        super(filters, messageRecursionLimit);
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
    protected Histogram getProcessingTimer() {
        return RAW_MSG_PROCESSING_TIME;
    }

    @Override
    protected String[] extractLabels(RawMessageBatch batch) {
        MessageID messageID = getMessages(batch).get(0).getMetadata().getId();
        return getSessionAliasAndDirection(messageID);
    }

    @Override
    protected int extractCountFrom(RawMessageBatch batch) {
        return batch.getMessagesCount();
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
                    throw new IllegalStateException("Message group batch contains parsed messages: " + MessageUtils.toJson(groupBatch));
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
    protected String toShortTraceString(RawMessageBatch value) {
        return MessageUtils.toJson(value);
    }

    @Override
    protected String toShortDebugString(RawMessageBatch value) {
        return getDebugString(getClass().getSimpleName(),
                value.getMessagesList().stream().map(message -> message.getMetadata().getId()).collect(Collectors.toList()));
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

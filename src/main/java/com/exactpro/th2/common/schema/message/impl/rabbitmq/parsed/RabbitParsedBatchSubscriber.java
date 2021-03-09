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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.parsed;

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.AnyMessage.KindCase;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.schema.message.MessageRouterUtils;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitBatchSubscriber;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

import java.util.ArrayList;
import java.util.List;

import static com.exactpro.th2.common.metrics.CommonMetrics.DEFAULT_BUCKETS;

public class RabbitParsedBatchSubscriber extends AbstractRabbitBatchSubscriber<Message, MessageBatch> {

    private static final Counter INCOMING_PARSED_MSG_BATCH_QUANTITY = Counter.build("th2_mq_incoming_parsed_msg_batch_quantity", "Quantity of incoming parsed message batches").register();
    private static final Counter INCOMING_PARSED_MSG_QUANTITY = Counter.build("th2_mq_incoming_parsed_msg_quantity", "Quantity of incoming parsed messages").register();
    private static final Histogram PARSED_MSG_PROCESSING_TIME = Histogram.build()
            .buckets(DEFAULT_BUCKETS)
            .name("th2_mq_parsed_msg_processing_time")
            .help("Time of processing parsed messages").register();

    @Override
    protected Counter getDeliveryCounter() {
        return INCOMING_PARSED_MSG_BATCH_QUANTITY;
    }

    @Override
    protected Counter getContentCounter() {
        return INCOMING_PARSED_MSG_QUANTITY;
    }

    @Override
    protected Histogram getProcessingTimer() {
        return PARSED_MSG_PROCESSING_TIME;
    }

    @Override
    protected int extractCountFrom(MessageBatch message) {
        return message.getMessagesCount();
    }

    public RabbitParsedBatchSubscriber(List<? extends RouterFilter> filters) {
        super(filters);
    }

    @Override
    protected List<MessageBatch> valueFromBytes(byte[] body) throws Exception {
        var groupBatch = MessageGroupBatch.parseFrom(body);
        var messageGroups = groupBatch.getGroupsList();
        var parsedBatches = new ArrayList<MessageBatch>(messageGroups.size());

        for (var group : messageGroups) {
            var builder = MessageBatch.newBuilder();

            for (AnyMessage message : group.getMessagesList()) {
                if (message.getKindCase() != KindCase.MESSAGE) {
                    throw new IllegalStateException("Message group batch contains raw messages: " + MessageRouterUtils.toJson(groupBatch));
                }

                builder.addMessages(message.getMessage());
            }

            parsedBatches.add(builder.build());
        }

        return parsedBatches;
    }

    @Override
    protected List<Message> getMessages(MessageBatch batch) {
        return batch.getMessagesList();
    }

    @Override
    protected MessageBatch createBatch(List<Message> messages) {
        return MessageBatch.newBuilder().addAllMessages(messages).build();
    }

    @Override
    protected String toShortDebugString(MessageBatch value) {
        return MessageRouterUtils.toJson(value);
    }

    @Override
    protected Metadata extractMetadata(Message message) {
        var metadata = message.getMetadata();
        var messageID = metadata.getId();
        return new Metadata(
                messageID.getSequence(),
                metadata.getMessageType(),
                messageID.getDirection(),
                messageID.getConnectionId().getSessionAlias()
        );
    }

}

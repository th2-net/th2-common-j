/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.SetUtils;
import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch.Builder;
import com.exactpro.th2.common.schema.filter.strategy.FilterStrategy;
import com.exactpro.th2.common.schema.filter.strategy.impl.Th2RawMsgFilterStrategy;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.router.AbstractRabbitBatchMessageRouter;
import com.google.protobuf.Message;

import io.prometheus.client.Counter;

public class RabbitRawBatchRouter extends AbstractRabbitBatchMessageRouter<RawMessage, RawMessageBatch, RawMessageBatch.Builder> {
    private static final Set<String> REQUIRED_SUBSCRIBE_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.RAW.toString(), QueueAttribute.SUBSCRIBE.toString());
    private static final Set<String> REQUIRED_SEND_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.RAW.toString(), QueueAttribute.PUBLISH.toString());

    private static final Counter OUTGOING_RAW_MSG_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_raw_msg_batch_quantity", "Quantity of outgoing raw message batches").register();
    private static final Counter OUTGOING_RAW_MSG_QUANTITY = Counter.build("th2_mq_outgoing_raw_msg_quantity", "Quantity of outgoing raw messages").register();

    @Override
    protected @NotNull FilterStrategy<Message> getDefaultFilterStrategy() {
        return new Th2RawMsgFilterStrategy();
    }

    @NotNull
    @Override
    protected Set<String> getRequiredSendAttributes() {
        return REQUIRED_SEND_ATTRIBUTES;
    }

    @NotNull
    @Override
    protected Set<String> getRequiredSubscribeAttributes() {
        return REQUIRED_SUBSCRIBE_ATTRIBUTES;
    }

    @Override
    protected List<RawMessage> getMessages(RawMessageBatch batch) {
        return batch.getMessagesList();
    }

    @Override
    protected Builder createBatchBuilder() {
        return RawMessageBatch.newBuilder();
    }

    @Override
    protected void addMessage(Builder builder, RawMessage message) {
        builder.addMessages(message);
    }

    @Override
    protected RawMessageBatch build(Builder builder) {
        return builder.build();
    }

    @NotNull
    @Override
    protected MessageSender<RawMessageBatch> createSender(QueueConfiguration queueConfiguration) {
        RabbitRawBatchSender rabbitRawBatchSender = new RabbitRawBatchSender();
        rabbitRawBatchSender.init(getConnectionManager(), queueConfiguration.getExchange(), queueConfiguration.getRoutingKey());
        return rabbitRawBatchSender;
    }

    @NotNull
    @Override
    protected MessageSubscriber<RawMessageBatch> createSubscriber(QueueConfiguration queueConfiguration) {
        RabbitRawBatchSubscriber rabbitRawBatchSubscriber = new RabbitRawBatchSubscriber(
                queueConfiguration.getFilters(),
                getConnectionManager().getConfiguration().getMessageRecursionLimit()
        );
        rabbitRawBatchSubscriber.init(
                getConnectionManager(),
                new SubscribeTarget(queueConfiguration.getQueue(), queueConfiguration.getRoutingKey(), queueConfiguration.getExchange()),
                this::filterMessage
        );
        return rabbitRawBatchSubscriber;
    }

    @NotNull
    @Override
    protected String toErrorString(RawMessageBatch rawMessageBatch) {
        return rawMessageBatch.toString();
    }

    @NotNull
    @Override
    protected Counter getDeliveryCounter() {
        return OUTGOING_RAW_MSG_BATCH_QUANTITY;
    }

    @NotNull
    @Override
    protected Counter getContentCounter() {
        return OUTGOING_RAW_MSG_QUANTITY;
    }

    @Override
    protected int extractCountFrom(RawMessageBatch batch) {
        return batch.getMessagesCount();
    }
}

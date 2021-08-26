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

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.SetUtils;
import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageBatch.Builder;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.router.AbstractRabbitBatchMessageRouter;

import io.prometheus.client.Counter;

public class RabbitParsedBatchRouter extends AbstractRabbitBatchMessageRouter<Message, MessageBatch, MessageBatch.Builder> {
    private static final Set<String> REQUIRED_SUBSCRIBE_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.PARSED.toString(), QueueAttribute.SUBSCRIBE.toString());
    private static final Set<String> REQUIRED_SEND_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.PARSED.toString(), QueueAttribute.PUBLISH.toString());

    private static final Counter OUTGOING_PARSED_MSG_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_parsed_msg_batch_quantity", "Quantity of outgoing parsed message batches").register();
    private static final Counter OUTGOING_PARSED_MSG_QUANTITY = Counter.build("th2_mq_outgoing_parsed_msg_quantity", "Quantity of outgoing parsed messages").register();

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
    protected List<Message> getMessages(MessageBatch batch) {
        return batch.getMessagesList();
    }

    @Override
    protected Builder createBatchBuilder() {
        return MessageBatch.newBuilder();
    }

    @Override
    protected void addMessage(Builder builder, Message message) {
        builder.addMessages(message);
    }

    @Override
    protected MessageBatch build(Builder builder) {
        return builder.build();
    }

    @NotNull
    @Override
    protected MessageSender<MessageBatch> createSender(QueueConfiguration queueConfiguration) {
        RabbitParsedBatchSender rabbitParsedBatchSender = new RabbitParsedBatchSender();
        rabbitParsedBatchSender.init(getConnectionManager(), queueConfiguration.getExchange(), queueConfiguration.getRoutingKey());
        return rabbitParsedBatchSender;
    }

    @NotNull
    @Override
    protected MessageSubscriber<MessageBatch> createSubscriber(QueueConfiguration queueConfiguration) {
        RabbitParsedBatchSubscriber rabbitParsedBatchSubscriber = new RabbitParsedBatchSubscriber(
                queueConfiguration.getFilters(),
                getConnectionManager().getConfiguration().getMessageRecursionLimit()
        );
        rabbitParsedBatchSubscriber.init(
                getConnectionManager(),
                new SubscribeTarget(queueConfiguration.getQueue(), queueConfiguration.getRoutingKey(), queueConfiguration.getExchange()),
                FilterFunction.DEFAULT_FILTER_FUNCTION
        );
        return rabbitParsedBatchSubscriber;
    }

    @NotNull
    @Override
    protected String toErrorString(MessageBatch messageBatch) {
        return messageBatch.toString();
    }

    @NotNull
    @Override
    protected Counter getDeliveryCounter() {
        return OUTGOING_PARSED_MSG_BATCH_QUANTITY;
    }

    @NotNull
    @Override
    protected Counter getContentCounter() {
        return OUTGOING_PARSED_MSG_QUANTITY;
    }

    @Override
    protected int extractCountFrom(MessageBatch batch) {
        return batch.getMessagesCount();
    }
}

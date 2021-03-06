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

import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageBatch.Builder;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.MessageQueue;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.router.AbstractRabbitBatchMessageRouter;
import org.apache.commons.collections4.SetUtils;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

public class RabbitParsedBatchRouter extends AbstractRabbitBatchMessageRouter<Message, MessageBatch, MessageBatch.Builder> {

    private static final Set<String> REQUIRED_SUBSCRIBE_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.PARSED.toString(), QueueAttribute.SUBSCRIBE.toString());
    private static final Set<String> REQUIRED_SEND_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.PARSED.toString(), QueueAttribute.PUBLISH.toString());

    @Override
    protected MessageQueue<MessageBatch> createQueue(@NotNull ConnectionManager connectionManager, @NotNull QueueConfiguration queueConfiguration, @NotNull FilterFunction filterFunction) {
        RabbitParsedBatchQueue queue = new RabbitParsedBatchQueue();
        queue.init(connectionManager, queueConfiguration, filterFunction);
        return queue;
    }

    @Override
    protected Set<String> requiredSubscribeAttributes() {
        return REQUIRED_SUBSCRIBE_ATTRIBUTES;
    }

    @Override
    protected Set<String> requiredSendAttributes() {
        return REQUIRED_SEND_ATTRIBUTES;
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
}

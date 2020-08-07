/*****************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/

package com.exactpro.th2.schema.message.impl.rabbitmq.parsed;

import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.MessageBatch.Builder;
import com.exactpro.th2.schema.message.MessageQueue;
import com.exactpro.th2.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.router.AbstractRabbitBatchMessageRouter;

import java.util.List;

public class RabbitParsedBatchRouter extends AbstractRabbitBatchMessageRouter<Message, MessageBatch, MessageBatch.Builder> {

    @Override
    protected MessageQueue<MessageBatch> createQueue(RabbitMQConfiguration configuration, QueueConfiguration queueConfiguration) {
        RabbitParsedBatchQueue queue = new RabbitParsedBatchQueue();
        queue.init(configuration, queueConfiguration);
        return queue;
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

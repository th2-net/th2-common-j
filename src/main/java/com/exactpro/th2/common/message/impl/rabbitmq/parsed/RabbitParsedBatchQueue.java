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
package com.exactpro.th2.common.message.impl.rabbitmq.parsed;

import java.util.Objects;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.message.MessageQueue;
import com.exactpro.th2.common.message.MessageSender;
import com.exactpro.th2.common.message.MessageSubscriber;
import com.exactpro.th2.common.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.infra.grpc.MessageBatch;

public class RabbitParsedBatchQueue implements MessageQueue<MessageBatch> {

    private RabbitMQConfiguration configuration = null;
    private QueueConfiguration queueConfiguration = null;

    @Override
    public void init(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration) {
        this.configuration = Objects.requireNonNull(configuration, "Rabbit MQ configuration can not be null");
        this.queueConfiguration = Objects.requireNonNull(queueConfiguration, "Queue configuration can not be null");
    }

    @Override
    public MessageSubscriber<MessageBatch> createSubscriber() {
        RabbitParsedBatchSubscriber result = new RabbitParsedBatchSubscriber();
        result.init(configuration, queueConfiguration.getExchangeName(), queueConfiguration.getName());
        return result;
    }

    @Override
    public MessageSender<MessageBatch> createSender() {
        RabbitParsedBatchSender result = new RabbitParsedBatchSender();
        result.init(configuration, queueConfiguration.getExchangeName(), queueConfiguration.getName());
        return result;
    }
}

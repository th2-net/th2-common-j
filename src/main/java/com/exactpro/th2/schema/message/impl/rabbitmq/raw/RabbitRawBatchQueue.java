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

package com.exactpro.th2.schema.message.impl.rabbitmq.raw;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.infra.grpc.RawMessageBatch;
import com.exactpro.th2.schema.message.MessageSender;
import com.exactpro.th2.schema.message.MessageSubscriber;
import com.exactpro.th2.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.AbstractRabbitQueue;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;

public class RabbitRawBatchQueue extends AbstractRabbitQueue<RawMessageBatch> {

    @Override
    protected MessageSender<RawMessageBatch> createSender(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration) {
        var result = new RabbitRawBatchSender();
        result.init(configuration, queueConfiguration.getExchange(), queueConfiguration.getName());
        return result;
    }

    @Override
    protected MessageSubscriber<RawMessageBatch> createSubscriber(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration) {
        var result = new RabbitRawBatchSubscriber();
        result.init(configuration, queueConfiguration.getExchange(), queueConfiguration.getName());
        return result;
    }
}

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

import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.schema.message.MessageSender;
import com.exactpro.th2.schema.message.MessageSubscriber;
import com.exactpro.th2.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.AbstractRabbitQueue;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import org.jetbrains.annotations.NotNull;

public class RabbitParsedBatchQueue extends AbstractRabbitQueue<MessageBatch> {

    @Override
    protected MessageSender<MessageBatch> createSender(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration) {
        RabbitParsedBatchSender result = new RabbitParsedBatchSender();
        result.init(configuration, queueConfiguration.getExchange(), queueConfiguration.getName());
        return result;
    }

    @Override
    protected MessageSubscriber<MessageBatch> createSubscriber(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration) {
        RabbitParsedBatchSubscriber result = new RabbitParsedBatchSubscriber(queueConfiguration.getFilters());
        var subscribeTarget = SubscribeTarget.builder()
                .routingKey(queueConfiguration.getName())
                .queue(queueConfiguration.getQueue())
                .build();
        result.init(configuration, queueConfiguration.getExchange(), subscribeTarget);
        return result;
    }
}

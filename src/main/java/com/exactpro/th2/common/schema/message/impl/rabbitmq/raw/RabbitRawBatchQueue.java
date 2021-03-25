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

import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.MessageRouterContext;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitQueue;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class RabbitRawBatchQueue extends AbstractRabbitQueue<RawMessageBatch> {


    public RabbitRawBatchQueue(@NotNull MessageRouterContext context, @NotNull QueueConfiguration queueConfiguration, @Nullable FilterFunction filterFunc) {
        super(context, queueConfiguration, filterFunc);
    }

    @Override
    protected MessageSender<RawMessageBatch> createSender(@NotNull MessageRouterContext context, @NotNull QueueConfiguration queueConfiguration) {
        return new RabbitRawBatchSender(context, queueConfiguration.getExchange(), queueConfiguration.getRoutingKey());
    }

    @Override
    protected MessageSubscriber<RawMessageBatch> createSubscriber(@NotNull MessageRouterContext context, @NotNull QueueConfiguration queueConfiguration, @NotNull FilterFunction filterFunction) {
        return new RabbitRawBatchSubscriber(context, new SubscribeTarget(queueConfiguration.getQueue(), queueConfiguration.getRoutingKey(), queueConfiguration.getExchange()), filterFunction, queueConfiguration.getFilters());
    }
}

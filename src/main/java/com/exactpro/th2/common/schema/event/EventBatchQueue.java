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

package com.exactpro.th2.common.schema.event;

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitQueue;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import org.jetbrains.annotations.NotNull;

public class EventBatchQueue extends AbstractRabbitQueue<EventBatch> {

    @Override
    protected MessageSender<EventBatch> createSender(@NotNull ConnectionManager connectionManager, @NotNull QueueConfiguration queueConfiguration) {
        EventBatchSender eventBatchSender = new EventBatchSender();
        eventBatchSender.init(connectionManager, queueConfiguration.getExchange(), queueConfiguration.getRoutingKey());
        return eventBatchSender;
    }

    @Override
    protected MessageSubscriber<EventBatch> createSubscriber(@NotNull ConnectionManager connectionManager, @NotNull QueueConfiguration queueConfiguration, @NotNull FilterFunction filterFunction) {
        EventBatchSubscriber eventBatchSubscriber = new EventBatchSubscriber();
        eventBatchSubscriber.init(connectionManager, new SubscribeTarget(queueConfiguration.getQueue(), queueConfiguration.getRoutingKey(), queueConfiguration.getExchange()), filterFunction);
        return eventBatchSubscriber;
    }
}

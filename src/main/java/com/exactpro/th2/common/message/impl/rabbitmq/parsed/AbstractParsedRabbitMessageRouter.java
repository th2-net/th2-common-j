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

import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.common.message.MessageListener;
import com.exactpro.th2.common.message.MessageQueue;
import com.exactpro.th2.common.message.SubscriberMonitor;
import com.exactpro.th2.common.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.message.impl.rabbitmq.AbstractRabbitMessageRouter;
import com.exactpro.th2.common.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.MessageFilter;

public abstract class AbstractParsedRabbitMessageRouter extends AbstractRabbitMessageRouter<MessageBatch> {

    @Override
    protected MessageQueue<MessageBatch> createQueue(RabbitMQConfiguration configuration, QueueConfiguration queueConfiguration) {
        RabbitParsedBatchQueue queue = new RabbitParsedBatchQueue();
        queue.init(configuration, queueConfiguration);
        return queue;
    }

    @Nullable
    @Override
    public SubscriberMonitor subscribe(MessageFilter filter, MessageListener<MessageBatch> callback) {
        return super.subscribe(filter, filter == null || StringUtils.isEmpty(filter.getConnectionId().getSessionAlias()) ? callback : (consumerTag, message) -> {
            callback.handler(consumerTag, MessageBatch
                    .newBuilder()
                    .addAllMessages(message
                                    .getMessagesList()
                                    .stream()
                                    .filter(message1 -> message1
                                            .getMetadata()
                                            .getId()
                                            .getConnectionId()
                                            .getSessionAlias()
                                            .equals(filter.getConnectionId().getSessionAlias())).collect(Collectors.toList())).build());
        });
    }
}

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
package com.exactpro.th2.common.message.impl.rabbitmq;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.message.MessageQueue;
import com.exactpro.th2.common.message.MessageSender;
import com.exactpro.th2.common.message.MessageSubscriber;
import com.exactpro.th2.common.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.message.impl.rabbitmq.configuration.RabbitMQConfiguration;

public abstract class AbstractRabbitQueue<T> implements MessageQueue<T> {

    private RabbitMQConfiguration configuration;
    private QueueConfiguration queueConfiguration;
    private MessageSubscriber<T> subscriber = null;
    private MessageSender<T> sender = null;

    @Override
    public void init(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration) {
        this.configuration = configuration;
        this.queueConfiguration = queueConfiguration;
    }

    @Override
    public synchronized MessageSubscriber<T> getSubscriber() {
        if (configuration == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue not yet init");
        }

        return subscriber == null ? subscriber = createSubscriber(configuration, queueConfiguration) : subscriber;
    }

    @Override
    public synchronized MessageSender<T> getSender() {
        if (configuration == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue not yet init");
        }

        return sender == null ? sender = createSender(configuration, queueConfiguration) : sender;
    }

    protected abstract MessageSender<T> createSender(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration);

    protected abstract MessageSubscriber<T> createSubscriber(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration);
}

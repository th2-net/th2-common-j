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

package com.exactpro.th2.schema.message.impl.rabbitmq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.schema.message.MessageQueue;
import com.exactpro.th2.schema.message.MessageSender;
import com.exactpro.th2.schema.message.MessageSubscriber;
import com.exactpro.th2.schema.message.configuration.QueueConfiguration;
import com.rabbitmq.client.Connection;

public abstract class AbstractRabbitQueue<T> implements MessageQueue<T> {
    private Connection connection;
    private String subscriberName;
    private QueueConfiguration queueConfiguration;
    private final List<AutoCloseable> resources = new CopyOnWriteArrayList<>();

    @Override
    public void init(@NotNull Connection connection, String subscriberName, @NotNull QueueConfiguration queueConfiguration) {
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");
        this.subscriberName = subscriberName;
        this.queueConfiguration = Objects.requireNonNull(queueConfiguration, "queueConfiguration cannot be null");
    }

    @Override
    public MessageSubscriber<T> getSubscriber() {
        if (connection == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue is not initialized");
        }

        if (!queueConfiguration.isCanRead()) {
            throw new IllegalStateException("Queue can not read");
        }

        MessageSubscriber<T> subscriber = createSubscriber(connection, subscriberName, queueConfiguration);

        resources.add(subscriber);

        return subscriber;
    }

    @Override
    public MessageSender<T> getSender() {
        if (connection == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue is not initialized");
        }

        if (!queueConfiguration.isCanWrite()) {
            throw new IllegalStateException("Queue can not write");
        }

        MessageSender<T> sender = createSender(connection, queueConfiguration);

        resources.add(sender);

        return sender;
    }

    @Override
    public void close() throws Exception {
        Collection<Exception> exceptions = new ArrayList<>();

        for (AutoCloseable resource : resources) {
            try {
                resource.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        resources.clear();

        if (!exceptions.isEmpty()) {
            Exception exception = new Exception("Can not close message queue");
            exceptions.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    protected abstract MessageSender<T> createSender(@NotNull Connection connection, @NotNull QueueConfiguration queueConfiguration);

    protected abstract MessageSubscriber<T> createSubscriber(@NotNull Connection connection, String subscriberName, @NotNull QueueConfiguration queueConfiguration);
}

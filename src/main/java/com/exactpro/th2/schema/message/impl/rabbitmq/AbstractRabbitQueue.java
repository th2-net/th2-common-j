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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.schema.message.MessageQueue;
import com.exactpro.th2.schema.message.MessageSender;
import com.exactpro.th2.schema.message.MessageSubscriber;
import com.exactpro.th2.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.connection.ConnectionOwner;

public abstract class AbstractRabbitQueue<T> implements MessageQueue<T> {

    private final AtomicReference<ConnectionOwner> connection = new AtomicReference<>();
    private final AtomicReference<QueueConfiguration> queueConfiguration = new AtomicReference<>();

    private final AtomicReference<MessageSender<T>> sender = new AtomicReference<>();
    private final AtomicReference<MessageSubscriber<T>> subscriber = new AtomicReference<>();

    @Override
    public void init(@NotNull ConnectionOwner connectionWrapper, @NotNull QueueConfiguration queueConfiguration) {
        if (this.connection.get() != null && this.queueConfiguration.get() != null) {
            throw new IllegalStateException("Queue is already initialize");
        }

        Objects.requireNonNull(connection, "Connection can not be null");
        Objects.requireNonNull(queueConfiguration, "Queue configuration can not be null");

        this.connection.updateAndGet(connection -> {
            if (connection == null) {
                connection = connectionWrapper;
            }
            return connection;
        });

        this.queueConfiguration.updateAndGet(configuration -> {
            if (configuration == null) {
                configuration = queueConfiguration;
            }
            return configuration;
        });
    }

    @Override
    public MessageSubscriber<T> getSubscriber() {
        ConnectionOwner connectionWrapper = connection.get();
        QueueConfiguration queueConfiguration = this.queueConfiguration.get();

        if (connectionWrapper == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue is not initialized");
        }

        if (!queueConfiguration.isCanRead()) {
            throw new IllegalStateException("Queue can not read");
        }

        return subscriber.updateAndGet( subscriber -> {
            if (subscriber == null) {
                subscriber = createSubscriber(connectionWrapper, queueConfiguration);
            }
            return subscriber;
        });
    }

    @Override
    public MessageSender<T> getSender() {
        ConnectionOwner connectionWrapper = connection.get();
        QueueConfiguration queueConfiguration = this.queueConfiguration.get();

        if (connectionWrapper == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue is not initialized");
        }

        if (!queueConfiguration.isCanWrite()) {
            throw new IllegalStateException("Queue can not write");
        }

        return sender.updateAndGet(sender -> {
            if (sender == null) {
                sender = createSender(connectionWrapper, queueConfiguration);
            }
            return sender;
        });
    }

    @Override
    public void close() throws Exception {
        Collection<Exception> exceptions = new ArrayList<>();


        subscriber.updateAndGet(subscriber -> {
            if (subscriber != null) {
                try {
                    subscriber.close();
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
            return null;
        });

        if (!exceptions.isEmpty()) {
            Exception exception = new Exception("Can not close message queue");
            exceptions.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    protected abstract MessageSender<T> createSender(@NotNull ConnectionOwner connectionOwner, @NotNull QueueConfiguration queueConfiguration);

    protected abstract MessageSubscriber<T> createSubscriber(@NotNull ConnectionOwner connectionOwner, @NotNull QueueConfiguration queueConfiguration);
}

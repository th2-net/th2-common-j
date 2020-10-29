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

package com.exactpro.th2.common.schema.message.impl.rabbitmq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.schema.message.MessageQueue;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;

public abstract class AbstractRabbitQueue<T> implements MessageQueue<T> {

    private final AtomicReference<ConnectionManager> connectionManager = new AtomicReference<>();
    private final AtomicReference<QueueConfiguration> queueConfiguration = new AtomicReference<>();

    private final AtomicReference<MessageSender<T>> sender = new AtomicReference<>();
    private final AtomicReference<MessageSubscriber<T>> subscriber = new AtomicReference<>();

    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull QueueConfiguration queueConfiguration) {
        Objects.requireNonNull(connectionManager, "Connection can not be null");
        Objects.requireNonNull(queueConfiguration, "Queue configuration can not be null");

        if (this.connectionManager.get() != null && this.queueConfiguration.get() != null) {
            throw new IllegalStateException("Queue is already initialize");
        }

        this.connectionManager.set(connectionManager);
        this.queueConfiguration.set(queueConfiguration);
    }

    @Override
    public MessageSubscriber<T> getSubscriber() {
        ConnectionManager connectionManger = connectionManager.get();
        QueueConfiguration queueConfiguration = this.queueConfiguration.get();

        if (connectionManger == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue is not initialized");
        }

        if (!queueConfiguration.isReadable()) {
            throw new IllegalStateException("Queue can not read");
        }

        return subscriber.updateAndGet( subscriber -> {
            if (subscriber == null) {
                return createSubscriber(connectionManger, queueConfiguration);
            }
            return subscriber;
        });
    }

    @Override
    public MessageSender<T> getSender() {
        ConnectionManager connectionManager = this.connectionManager.get();
        QueueConfiguration queueConfiguration = this.queueConfiguration.get();

        if (connectionManager == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue is not initialized");
        }

        if (!queueConfiguration.isWritable()) {
            throw new IllegalStateException("Queue can not write");
        }

        return sender.updateAndGet(sender -> {
            if (sender == null) {
                return createSender(connectionManager, queueConfiguration);
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

    protected abstract MessageSender<T> createSender(@NotNull ConnectionManager connectionManager, @NotNull QueueConfiguration queueConfiguration);

    protected abstract MessageSubscriber<T> createSubscriber(@NotNull ConnectionManager connectionManager, @NotNull QueueConfiguration queueConfiguration);
}

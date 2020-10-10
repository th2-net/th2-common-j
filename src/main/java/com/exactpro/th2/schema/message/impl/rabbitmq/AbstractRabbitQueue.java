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
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.schema.message.MessageQueue;
import com.exactpro.th2.schema.message.MessageSender;
import com.exactpro.th2.schema.message.MessageSubscriber;
import com.exactpro.th2.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.connection.ConnectionOwner;
import com.rabbitmq.client.Connection;

public abstract class AbstractRabbitQueue<T> implements MessageQueue<T> {
    private ConnectionOwner connectionOwner;
    private String subscriberName;
    private QueueConfiguration queueConfiguration;
    private final AtomicReference<MessageSender<T>> sender = new AtomicReference<>(null);
    private final AtomicReference<MessageSubscriber<T>> subscriber = new AtomicReference<>(null);

    @Override
    public void init(@NotNull ConnectionOwner connectionOwner, @NotNull QueueConfiguration queueConfiguration) {
        this.connectionOwner = Objects.requireNonNull(connectionOwner, "connection cannot be null");
        this.subscriberName = connectionOwner.getSubscriberName();
        this.queueConfiguration = Objects.requireNonNull(queueConfiguration, "queueConfiguration cannot be null");
    }

    @Override
    public MessageSubscriber<T> getSubscriber() {
        if (connectionOwner == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue is not initialized");
        }

        if (!queueConfiguration.isCanRead()) {
            throw new IllegalStateException("Queue can not read");
        }

        return subscriber.updateAndGet(subscriber -> {
            if (subscriber == null) {
                subscriber = createSubscriber(connectionOwner, subscriberName, queueConfiguration);
            }
            return subscriber;
        });
    }

    @Override
    public MessageSender<T> getSender() {
        if (connectionOwner == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue is not initialized");
        }

        if (!queueConfiguration.isCanWrite()) {
            throw new IllegalStateException("Queue can not write");
        }

        return sender.updateAndGet(sender -> {
            if (sender == null) {
                sender = createSender(connectionOwner, queueConfiguration);
            }
            return sender;
        });
    }

    @Override
    public void close() throws Exception {
        Collection<Exception> exceptions = new ArrayList<>();

        subscriber.updateAndGet(subscriber -> {
            try {
                subscriber.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
            return null;
        });

        sender.updateAndGet(sender -> {
            try {
                sender.close();
            } catch (Exception e) {
                exceptions.add(e);
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

    protected abstract MessageSubscriber<T> createSubscriber(@NotNull ConnectionOwner connectionOwner, String subscriberName, @NotNull QueueConfiguration queueConfiguration);
}

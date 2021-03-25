/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq;

import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.MessageQueue;
import com.exactpro.th2.common.schema.message.MessageRouterContext;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractRabbitQueue<T> implements MessageQueue<T> {

    private final MessageRouterContext context;
    private final ConnectionManager connectionManager;
    private final QueueConfiguration queueConfiguration;
    private final FilterFunction filter;
    private final AtomicReference<MessageSender<T>> sender = new AtomicReference<>();
    private final AtomicReference<MessageSubscriber<T>> subscriber = new AtomicReference<>();

    public AbstractRabbitQueue(@NotNull MessageRouterContext context, @NotNull QueueConfiguration queueConfiguration, @Nullable FilterFunction filterFunc) {
        this.context = Objects.requireNonNull(context, "Context can not be null");
        this.connectionManager = context.getConnectionManager();
        this.queueConfiguration = Objects.requireNonNull(queueConfiguration, "Queue configuration can not be null");
        this.filter = ObjectUtils.defaultIfNull(filterFunc, FilterFunction.DEFAULT_FILTER_FUNCTION);
    }

    @Override
    public MessageSubscriber<T> getSubscriber() {
        if (!queueConfiguration.isReadable()) {
            throw new IllegalStateException("Queue can not read");
        }

        return subscriber.updateAndGet( subscriber -> {
            if (subscriber == null) {
                return createSubscriber(context, queueConfiguration, filter);
            }
            return subscriber;
        });
    }

    @Override
    public MessageSender<T> getSender() {
        if (!queueConfiguration.isWritable()) {
            throw new IllegalStateException("Queue can not write");
        }

        return sender.updateAndGet(sender -> {
            if (sender == null) {
                return createSender(context, queueConfiguration);
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

    protected abstract MessageSender<T> createSender(@NotNull MessageRouterContext context, @NotNull QueueConfiguration queueConfiguration);

    protected abstract MessageSubscriber<T> createSubscriber(@NotNull MessageRouterContext context, @NotNull QueueConfiguration queueConfiguration, @NotNull FilterFunction filterFunction);
}

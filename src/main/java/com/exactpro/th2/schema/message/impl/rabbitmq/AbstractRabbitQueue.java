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

import com.exactpro.th2.schema.message.MessageQueue;
import com.exactpro.th2.schema.message.MessageSender;
import com.exactpro.th2.schema.message.MessageSubscriber;
import com.exactpro.th2.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public abstract class AbstractRabbitQueue<T> implements MessageQueue<T> {

    private RabbitMQConfiguration configuration;
    private QueueConfiguration queueConfiguration;
    private final Object subscriberLock = new Object();
    private final Object senderLock = new Object();
    private MessageSubscriber<T> subscriber = null;
    private MessageSender<T> sender = null;

    @Override
    public void init(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration) {
        this.configuration = configuration;
        this.queueConfiguration = queueConfiguration;
    }

    @Override
    public MessageSubscriber<T> getSubscriber() {
        if (configuration == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue not yet init");
        }

        if (!queueConfiguration.isCanRead()) {
            throw new IllegalStateException("Queue can not read");
        }
        synchronized (subscriberLock) {
            return subscriber == null || subscriber.isClose() ? subscriber = createSubscriber(configuration, queueConfiguration) : subscriber;
        }
    }

    @Override
    public MessageSender<T> getSender() {
        if (configuration == null || queueConfiguration == null) {
            throw new IllegalStateException("Queue not yet init");
        }

        if (!queueConfiguration.isCanWrite()) {
            throw new IllegalStateException("Queue can not write");
        }
        synchronized (senderLock) {
            return sender == null || sender.isClose() ? sender = createSender(configuration, queueConfiguration) : sender;
        }
    }

    @Override
    public void close() throws IOException {
        IOException exception = new IOException("Can not close message queue");

        synchronized (subscriberLock) {
            if (subscriber != null && !subscriber.isClose()) {
                try {
                    subscriber.close();
                } catch (IOException e) {
                    exception.addSuppressed(e);
                }
            }
        }

        synchronized (senderLock) {
            if (sender != null && !sender.isClose()) {
                try {
                    sender.close();
                } catch (IOException e) {
                    exception.addSuppressed(e);
                }
            }
        }

        if (exception.getSuppressed().length > 0) {
            throw exception;
        }
    }

    protected abstract MessageSender<T> createSender(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration);

    protected abstract MessageSubscriber<T> createSubscriber(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration);
}

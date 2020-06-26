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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.common.message.MessageListener;
import com.exactpro.th2.common.message.MessageQueue;
import com.exactpro.th2.common.message.MessageRouter;
import com.exactpro.th2.common.message.MessageSubscriber;
import com.exactpro.th2.common.message.SubscriberMonitor;
import com.exactpro.th2.common.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.infra.grpc.MessageFilter;

public abstract class AbstractRabbitMessageRouter<T> implements MessageRouter<T> {

    protected MessageRouterConfiguration configuration;
    private RabbitMQConfiguration rabbitMQConfiguration;
    private Map<String, MessageQueue<T>> queueConnections = new HashMap<>();

    @Override
    public void init(RabbitMQConfiguration rabbitMQConfiguration, MessageRouterConfiguration configuration) {
        this.configuration = configuration;
        this.rabbitMQConfiguration = rabbitMQConfiguration;
    }

    @Nullable
    @Override
    public SubscriberMonitor subscribe(MessageFilter filter, MessageListener<T> callback) {
        return subscribe(callback, configuration.getQueueAliasByMesageFilter(filter).toArray(new String[0]));
    }

    @Override
    public SubscriberMonitor subscribe(String queueAlias, MessageListener<T> callback) {
        var queue = getMessageQueue(queueAlias);
        MessageSubscriber<T> subscriber = queue.getSubscriber();
        subscriber.addListener(callback);
        return new SubscriberMonitorImpl(subscriber, queue);
    }

    @Nullable
    @Override
    public SubscriberMonitor subscribe(MessageListener<T> callback, String... queueTags) {
        return new MultiplySubscribeMonitorImpl(configuration.getQueuesAliasByAttribute(queueTags).stream().map(alias -> subscribe(alias, callback)).collect(Collectors.toList()));
    }

    @Override
    public SubscriberMonitor subscribeAll(MessageListener<T> callback) {
        return new MultiplySubscribeMonitorImpl(configuration.getQueues().keySet().stream().map(alias -> subscribe(alias, callback)).collect(Collectors.toList()));
    }

    @Override
    public void send(T message) throws IOException {
        IOException exception = null;
        for (String targetQueueAlias : getTargetQueueAliasesForSend(message)) {
            try {
                send(targetQueueAlias, message);
            } catch (IOException e) {
                if (exception == null) {
                    exception = new IOException("Can not send to some queue");
                }
                exception.addSuppressed(e);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void send(T message, String... queueAttr) throws IOException {
        Set<String> queuesAliases = configuration.getQueuesAliasByAttribute(queueAttr);
        if (queuesAliases.size() > 1) {
            throw new IllegalStateException("Wrong size of queues aliases for send. Not more then 1");
        }

        for (String queuesAlias : queuesAliases) {
            send(queuesAlias, message);
        }
    }

    protected abstract MessageQueue<T> createQueue(RabbitMQConfiguration configuration, QueueConfiguration queueConfiguration);

    protected abstract List<String> getTargetQueueAliasesForSend(T message);

    protected void send(String queueAlias, T value) throws IOException {
        getMessageQueue(queueAlias).getSender().send(value);
    }

    protected MessageQueue<T> getMessageQueue(String queueAlias) {
        synchronized (queueConnections) {
            return queueConnections.computeIfAbsent(queueAlias, key -> createQueue(rabbitMQConfiguration, configuration.getQueueByAlias(key)));
        }
    }

    protected static class SubscriberMonitorImpl implements SubscriberMonitor {

        private final Object lock;
        private final MessageSubscriber<?> subscriber;

        public SubscriberMonitorImpl(@NotNull MessageSubscriber<?> subscriber, @Nullable Object lock) {
            this.lock = lock == null ? subscriber : lock;
            this.subscriber = subscriber;
        }

        @Override
        public void unsubscribe() throws IOException {
            synchronized (lock) {
                subscriber.close();
            }
        }
    }

    protected static class MultiplySubscribeMonitorImpl implements SubscriberMonitor {

        private final List<SubscriberMonitor> subscriberMonitors;

        public MultiplySubscribeMonitorImpl(List<SubscriberMonitor> subscriberMonitors) {
            this.subscriberMonitors = subscriberMonitors;
        }

        @Override
        public void unsubscribe() throws IOException {
            IOException exception = null;
            for (SubscriberMonitor monitor : subscriberMonitors) {
                try {
                    monitor.unsubscribe();
                } catch (IOException e) {
                    if (exception == null) {
                        exception = new IOException("Can not unsubscribe from some subscribe monitors");
                    }
                    exception.addSuppressed(e);
                }
            }
            if (exception != null) {
                throw exception;
            }
        }
    }
}

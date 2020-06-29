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
package com.exactpro.th2.common.message.impl.rabbitmq.router;

import com.exactpro.th2.common.filter.factory.FilterFactory;
import com.exactpro.th2.common.filter.factory.impl.DefaultFilterFactory;
import com.exactpro.th2.common.message.*;
import com.exactpro.th2.common.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.infra.grpc.MessageFilter;
import com.google.protobuf.Message;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractRabbitMessageRouter<T extends Message> implements MessageRouter<T> {

    protected FilterFactory filterFactory;
    protected MessageRouterConfiguration configuration;
    private RabbitMQConfiguration rabbitMQConfiguration;
    private Map<String, MessageQueue<T>> queueConnections = new HashMap<>();

    @Override
    public void init(RabbitMQConfiguration rabbitMQConfiguration, MessageRouterConfiguration configuration) {
        this.configuration = configuration;
        this.rabbitMQConfiguration = rabbitMQConfiguration;
        this.filterFactory = new DefaultFilterFactory();
    }

    @Nullable
    @Override
    public SubscriberMonitor subscribe(MessageFilter filter, MessageListener<T> callback) {
        List<SubscriberMonitor> queues = configuration.getQueueAliasByMessageFilter(filter).stream().map(alias -> subscribe(alias, callback)).collect(Collectors.toList());
        return queues.size() < 1 ? null : new MultiplySubscribeMonitorImpl(queues);
    }

    @Nullable
    @Override
    public SubscriberMonitor subscribe(MessageFilter filter, MessageListener<T> callback, String... queueAttr) {
        var queues = CollectionUtils.intersection(configuration.getQueueAliasByMessageFilter(filter), configuration.getQueuesAliasByAttribute(queueAttr)).stream().map(alias -> subscribe(alias, callback)).collect(Collectors.toList());
        return queues.size() < 1 ? null : new MultiplySubscribeMonitorImpl(queues);
    }

    @Nullable
    @Override
    public SubscriberMonitor subscribe(String queueAlias, MessageListener<T> callback) {
        var queue = getMessageQueue(queueAlias);
        MessageSubscriber<T> subscriber = queue.getSubscriber();
        subscriber.addListener(callback);
        return new SubscriberMonitorImpl(subscriber, queue);
    }

    @Nullable
    @Override
    public SubscriberMonitor subscribe(MessageListener<T> callback, String... queueAttr) {
        List<SubscriberMonitor> queues = configuration.getQueuesAliasByAttribute(queueAttr).stream().map(alias -> subscribe(alias, callback)).collect(Collectors.toList());
        return queues.size() < 1 ? null : new MultiplySubscribeMonitorImpl(queues);
    }

    @Override
    public SubscriberMonitor subscribeAll(MessageListener<T> callback) {
        return new MultiplySubscribeMonitorImpl(configuration.getQueues().keySet().stream().map(alias -> subscribe(alias, callback)).collect(Collectors.toList()));
    }

    @Override
    public void send(T message) throws IOException {
        IOException exception = null;

        for (var targetAliasesAndBatch : getTargetQueueAliasesAndBatchesForSend(message).entrySet()) {
           var targetAliases = targetAliasesAndBatch.getKey();
           var batch = targetAliasesAndBatch.getValue();

            for (var targetAlias: targetAliases) {
                try {
                    send(targetAlias, batch);
                } catch (IOException e) {
                    if (exception == null) {
                        exception = new IOException("Can not send to some queue");
                    }
                    exception.addSuppressed(e);
                }
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

    /**
     * Sets a fields filter factory
     *
     * @param filterFactory filter factory for filtering message fields
     * @throws NullPointerException if {@code filterFactory} is null
     */
    public void setFilterFactory(FilterFactory filterFactory) {
        Objects.requireNonNull(filterFactory);
        this.filterFactory = filterFactory;
    }


    protected abstract MessageQueue<T> createQueue(RabbitMQConfiguration configuration, QueueConfiguration queueConfiguration);

    protected abstract Map<Set<String>, T> getTargetQueueAliasesAndBatchesForSend(T message);

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

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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.exception.RouterException;
import com.exactpro.th2.schema.filter.strategy.FilterStrategy;
import com.exactpro.th2.schema.filter.strategy.impl.DefaultFilterStrategy;
import com.exactpro.th2.schema.message.MessageListener;
import com.exactpro.th2.schema.message.MessageQueue;
import com.exactpro.th2.schema.message.MessageRouter;
import com.exactpro.th2.schema.message.MessageSender;
import com.exactpro.th2.schema.message.MessageSubscriber;
import com.exactpro.th2.schema.message.SubscriberMonitor;
import com.exactpro.th2.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;

public abstract class AbstractRabbitMessageRouter<T> implements MessageRouter<T> {
    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected FilterStrategy filterStrategy;

    private MessageRouterConfiguration configuration;
    private RabbitMQConfiguration rabbitMQConfiguration;
    private Map<String, MessageQueue<T>> queueConnections = new HashMap<>();

    @Override
    public void init(@NotNull RabbitMQConfiguration rabbitMQConfiguration, @NotNull MessageRouterConfiguration configuration) {
        this.configuration = configuration;
        this.rabbitMQConfiguration = rabbitMQConfiguration;
        this.filterStrategy = new DefaultFilterStrategy();
    }

    @Nullable
    @Override
    public SubscriberMonitor subscribe(String queueAlias, MessageListener<T> callback) {
        var queue = getMessageQueue(queueAlias);
        MessageSubscriber<T> subscriber = queue.getSubscriber();
        subscriber.addListener(callback);

        try {
            subscriber.start();
        } catch (Exception e) {
            throw new RouterException("Can not start subscriber", e);
        }

        return new SubscriberMonitorImpl(subscriber, queue);
    }

    @Nullable
    @Override
    public SubscriberMonitor subscribe(MessageListener<T> callback, String... queueAttr) {
        var queues = configuration.findQueuesByAttr(queueAttr);
        if (queues.size() > 1) {
            throw new IllegalStateException("Wrong size of queues aliases for send. Not more then 1");
        }

        return queues.size() < 1 ? null : subscribe(queues.keySet().iterator().next(), callback);
    }

    @Override
    public SubscriberMonitor subscribeAll(MessageListener<T> callback) {
        List<SubscriberMonitor> subscribers = configuration.getQueues().keySet().stream().map(alias -> subscribe(alias, callback)).collect(Collectors.toList());
        return subscribers.isEmpty() ? null : new MultiplySubscribeMonitorImpl(subscribers);
    }

    @Override
    public SubscriberMonitor subscribeAll(MessageListener<T> callback, String... queueAttr) {
        List<SubscriberMonitor> subscribers = configuration.findQueuesByAttr(queueAttr).keySet().stream().map(queueConfiguration -> subscribe(queueConfiguration, callback)).collect(Collectors.toList());
        return subscribers.isEmpty() ? null : new MultiplySubscribeMonitorImpl(subscribers);
    }

    @Override
    public void unsubscribeAll() throws IOException {
        IOException exception = new IOException("Can not close message router");

        synchronized (queueConnections) {
            for (MessageQueue<T> queue : queueConnections.values()) {
                try {
                    queue.close();
                } catch (IOException e) {
                    exception.addSuppressed(e);
                }
            }

            queueConnections.clear();
        }

        if (exception.getSuppressed().length > 0) {
            throw exception;
        }
    }

    @Override
    public void send(T message) throws IOException {
        send(findByFilter(configuration.getQueues(), message));
    }

    @Override
    public void send(T message, String... queueAttr) throws IOException {

        var filteredByAttr = configuration.findQueuesByAttr(queueAttr);

        var filteredByAttrAndFilter = findByFilter(filteredByAttr, message);

        if (filteredByAttrAndFilter.size() != 1) {
            throw new IllegalStateException("Wrong size of queues for send. Should be equal to 1");
        }

        send(filteredByAttrAndFilter);
    }

    @Override
    public void sendAll(T message, String... queueAttr) throws IOException {

        var filteredByAttr = configuration.findQueuesByAttr(queueAttr);

        var filteredByAttrAndFilter = findByFilter(filteredByAttr, message);

        if (filteredByAttrAndFilter.size() == 0) {
            throw new IllegalStateException("Wrong size of queues for send. Can't be equal to 0");
        }

        send(filteredByAttrAndFilter);
    }

    /**
     * Sets a fields filter strategy
     *
     * @param filterStrategy filter strategy for filtering message fields
     * @throws NullPointerException if {@code filterStrategy} is null
     */
    public void setFilterStrategy(FilterStrategy filterStrategy) {
        Objects.requireNonNull(filterStrategy);
        this.filterStrategy = filterStrategy;
    }

    @Override
    public void close() throws Exception {
        logger.info("Closing message router");
        unsubscribeAll();
        logger.info("Message router has been successfully closed");
    }

    protected abstract MessageQueue<T> createQueue(RabbitMQConfiguration configuration, QueueConfiguration queueConfiguration);

    protected abstract Map<String, T> findByFilter(Map<String, QueueConfiguration> queues, T msg);


    protected void send(Map<String, T> aliasesAndMessagesToSend) throws IOException {
        IOException exception = new IOException("Can not send to some queue");

        for (var targetAliasesAndBatch : aliasesAndMessagesToSend.entrySet()) {
            var queueAlias = targetAliasesAndBatch.getKey();
            var message = targetAliasesAndBatch.getValue();

            try {
                MessageSender<T> sender = getMessageQueue(queueAlias).getSender();
                sender.start();
                sender.send(message);
            } catch (IOException e) {
                exception.addSuppressed(e);
            } catch (Exception e) {
                throw new RouterException("Can not start sender");
            }
        }

        if (exception.getSuppressed().length > 0) {
            throw exception;
        }
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

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
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
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import lombok.val;

public abstract class AbstractRabbitMessageRouter<T> implements MessageRouter<T> {
    private static final int CHANNEL_CLOSE_TIMEOUT_MS = 1000;

    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected FilterStrategy filterStrategy;

    private MessageRouterConfiguration configuration;
    private String subscriberName;
    private Connection connection;
    private final ConcurrentMap<String, MessageQueue<T>> queueConnections = new ConcurrentHashMap<>();

    @Override
    public void init(@NotNull RabbitMQConfiguration rabbitMQConfiguration, @NotNull MessageRouterConfiguration configuration) {
        Objects.requireNonNull(rabbitMQConfiguration, "RabbitMQ configuration cannot be null");

        this.configuration = Objects.requireNonNull(configuration, "configuration cannot be null");
        this.filterStrategy = new DefaultFilterStrategy();
        this.subscriberName = rabbitMQConfiguration.getSubscriberName();

        val factory = new ConnectionFactory();
        val virtualHost = rabbitMQConfiguration.getvHost();
        val username = rabbitMQConfiguration.getUsername();
        val password = rabbitMQConfiguration.getPassword();

        factory.setHost(rabbitMQConfiguration.getHost());
        factory.setPort(rabbitMQConfiguration.getPort());

        if (StringUtils.isNotBlank(virtualHost)) {
            factory.setVirtualHost(virtualHost);
        }

        if (StringUtils.isNotBlank(username)) {
            factory.setUsername(username);
        }

        if (StringUtils.isNotBlank(password)) {
            factory.setPassword(password);
        }

        try {
            this.connection = factory.newConnection();
        } catch (IOException | TimeoutException e) {
            throw new RouterException("Failed to create RabbitMQ connection using following configuration: " + rabbitMQConfiguration, e);
        }
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

    @NotNull
    @Override
    public SubscriberMonitor subscribe(MessageListener<T> callback, String... queueAttr) {
        var queues = configuration.findQueuesByAttr(queueAttr);
        if (queues.size() != 1) {
            throw new IllegalStateException("Wrong size of queues aliases for subscribe. Should be equal to 1");
        }

        return subscribe(queues.keySet().iterator().next(), callback);
    }

    @Override
    public SubscriberMonitor subscribeAll(MessageListener<T> callback) {
        List<SubscriberMonitor> subscribers = configuration.getQueues().keySet().stream().map(alias -> subscribe(alias, callback)).collect(Collectors.toList());
        if (subscribers.isEmpty()) {
            throw new IllegalStateException("Wrong size of queues aliases for subscribeAll. Should not be empty");
        }
        return new MultiplySubscribeMonitorImpl(subscribers);
    }

    @Override
    public SubscriberMonitor subscribeAll(MessageListener<T> callback, String... queueAttr) {
        List<SubscriberMonitor> subscribers = configuration.findQueuesByAttr(queueAttr).keySet().stream().map(queueConfiguration -> subscribe(queueConfiguration, callback)).collect(Collectors.toList());
        if (subscribers.isEmpty()) {
            throw new IllegalStateException("Wrong size of queues aliases for subscribeAll. Should not be empty");
        }
        return new MultiplySubscribeMonitorImpl(subscribers);
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

        if (filteredByAttrAndFilter.isEmpty()) {
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
    public void close() {
        logger.info("Closing message router");

        Collection<Exception> exceptions = new ArrayList<>();

        for (MessageQueue<T> queue : queueConnections.values()) {
            try {
                queue.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        queueConnections.clear();

        try {
            connection.close(CHANNEL_CLOSE_TIMEOUT_MS);
        } catch (IOException e) {
            exceptions.add(e);
        }

        if (!exceptions.isEmpty()) {
            RuntimeException exception = new RouterException("Can not close message router");
            exceptions.forEach(exception::addSuppressed);
            throw exception;
        }

        logger.info("Message router has been successfully closed");
    }

    protected abstract MessageQueue<T> createQueue(Connection connection, String subscriberName, QueueConfiguration queueConfiguration);

    protected abstract Map<String, T> findByFilter(Map<String, QueueConfiguration> queues, T msg);

    protected void send(Map<String, T> aliasesAndMessagesToSend) {
        Collection<Exception> exceptions = new ArrayList<>();

        aliasesAndMessagesToSend.forEach((queueAlias, message) -> {
            try {
                MessageSender<T> sender = getMessageQueue(queueAlias).getSender();
                sender.start();
                sender.send(message);
            } catch (IOException e) {
                exceptions.add(e);
            } catch (Exception e) {
                throw new RouterException("Can not start sender to queue: " + queueAlias, e);
            }
        });

        if (!exceptions.isEmpty()) {
            RouterException exception = new RouterException("Can not send to some queue");
            exceptions.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    protected MessageQueue<T> getMessageQueue(String queueAlias) {
        return queueConnections.computeIfAbsent(queueAlias, key -> createQueue(connection, subscriberName, configuration.getQueueByAlias(key)));
    }

    protected static class SubscriberMonitorImpl implements SubscriberMonitor {

        private final Object lock;
        private final MessageSubscriber<?> subscriber;

        public SubscriberMonitorImpl(@NotNull MessageSubscriber<?> subscriber, @Nullable Object lock) {
            this.lock = lock == null ? subscriber : lock;
            this.subscriber = subscriber;
        }

        @Override
        public void unsubscribe() throws Exception {
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
        public void unsubscribe() throws Exception {
            Exception exception = null;
            for (SubscriberMonitor monitor : subscriberMonitors) {
                try {
                    monitor.unsubscribe();
                } catch (Exception e) {
                    if (exception == null) {
                        exception = new Exception("Can not unsubscribe from some subscribe monitors");
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

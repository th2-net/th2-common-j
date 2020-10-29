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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.schema.exception.RouterException;
import com.exactpro.th2.common.schema.filter.strategy.FilterStrategy;
import com.exactpro.th2.common.schema.filter.strategy.impl.DefaultFilterStrategy;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageQueue;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;

public abstract class AbstractRabbitMessageRouter<T> implements MessageRouter<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final AtomicReference<ConnectionManager> connection = new AtomicReference<>();
    private final AtomicReference<MessageRouterConfiguration> configuration = new AtomicReference<>();
    protected final AtomicReference<FilterStrategy> filterStrategy = new AtomicReference<>(new DefaultFilterStrategy());

    private final ConcurrentMap<String, MessageQueue<T>> queueConnections = new ConcurrentHashMap<>();

    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull MessageRouterConfiguration messageRouterConfiguration) {
        Objects.requireNonNull(connectionManager, "Connection owner can not be null");
        Objects.requireNonNull(configuration, "configuration cannot be null");

        if (this.connection.get() != null || this.configuration.get() != null) {
            throw new IllegalStateException("Router is already initialized");
        }

        this.connection.set(connectionManager);
        this.configuration.set(messageRouterConfiguration);
    }

    @Nullable
    private SubscriberMonitor subscribe(String queueAlias, MessageListener<T> callback) {
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
        var attributes = addRequiredSubscribeAttributes(queueAttr);
        var queues = getConfiguration().findQueuesByAttr(attributes);
        if (queues.size() != 1) {
            throw new IllegalStateException("Wrong amount of queues for subscribe. Found " + queues.size() + " queues, but must not be more than 1. Search was done by " + attributes + " attributes");
        }
        return subscribe(queues.keySet().iterator().next(), callback);
    }

    @Override
    public SubscriberMonitor subscribeAll(MessageListener<T> callback) {
        List<SubscriberMonitor> subscribers = getConfiguration().findQueuesByAttr(requiredSubscribeAttributes()).keySet().stream().map(alias -> subscribe(alias, callback)).collect(Collectors.toList());
        if (subscribers.isEmpty()) {
            throw new IllegalStateException("Wrong amount of queues for subscribeAll. Should not be empty. Search was done by " + requiredSubscribeAttributes() + " attributes");
        }
        return new MultiplySubscribeMonitorImpl(subscribers);
    }

    @Override
    public SubscriberMonitor subscribeAll(MessageListener<T> callback, String... queueAttr) {
        var attributes = addRequiredSubscribeAttributes(queueAttr);
        List<SubscriberMonitor> subscribers = getConfiguration().findQueuesByAttr(attributes).keySet().stream().map(queueConfiguration -> subscribe(queueConfiguration, callback)).collect(Collectors.toList());
        if (subscribers.isEmpty()) {
            throw new IllegalStateException("Wrong amount of queues for subscribeAll. Should not be empty. Search was done by " + attributes + " attributes");
        }
        return new MultiplySubscribeMonitorImpl(subscribers);
    }

    @Override
    public void send(T message) throws IOException {
        var filteredByAttrAndFilter = findByFilter(requiredSendAttributes().size() > 0 ? getConfiguration().findQueuesByAttr(requiredSendAttributes()) : getConfiguration().getQueues(), message);

        if (filteredByAttrAndFilter.size() != 1) {
            throw new IllegalStateException("Wrong count of queues for send. Should be equal to 1. Find queues = " + filteredByAttrAndFilter.size());
        }

        send(filteredByAttrAndFilter);
    }

    @Override
    public void send(T message, String... queueAttr) throws IOException {

        var filteredByAttr = getConfiguration().findQueuesByAttr(addRequiredSendAttributes(queueAttr));

        var filteredByAttrAndFilter = findByFilter(filteredByAttr, message);

        if (filteredByAttrAndFilter.size() != 1) {
            throw new IllegalStateException("Wrong size of queues for send. Should be equal to 1");
        }

        send(filteredByAttrAndFilter);
    }

    @Override
    public void sendAll(T message, String... queueAttr) throws IOException {

        var filteredByAttr = getConfiguration().findQueuesByAttr(addRequiredSendAttributes(queueAttr));

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
        this.filterStrategy.set(Objects.requireNonNull(filterStrategy));
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

        if (!exceptions.isEmpty()) {
            RuntimeException exception = new RouterException("Can not close message router");
            exceptions.forEach(exception::addSuppressed);
            throw exception;
        }

        logger.info("Message router has been successfully closed");
    }

    protected abstract MessageQueue<T> createQueue(@NotNull ConnectionManager connectionManager, @NotNull QueueConfiguration queueConfiguration);

    protected abstract Map<String, T> findByFilter(Map<String, QueueConfiguration> queues, T msg);

    protected abstract Set<String> requiredSubscribeAttributes();

    protected abstract Set<String> requiredSendAttributes();

    protected void send(Map<String, T> aliasesAndMessagesToSend) {
        Collection<Exception> exceptions = new ArrayList<>();

        aliasesAndMessagesToSend.forEach((queueAlias, message) -> {
            try {
                MessageSender<T> sender = getMessageQueue(queueAlias).getSender();
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
        return queueConnections.computeIfAbsent(queueAlias, key -> {
            ConnectionManager connectionManager = connection.get();
            if (connectionManager == null) {
                throw new IllegalStateException("Router is not initialized");
            }

            QueueConfiguration queueByAlias = getConfiguration().getQueueByAlias(key);
            if (queueByAlias == null) {
                throw new IllegalStateException("Can not find queue");
            }

            return createQueue(connectionManager, queueByAlias);
        });
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

    private Collection<String> addRequiredSubscribeAttributes(String[] queueAttr) {
        Set<String> attributes = new HashSet<>(requiredSubscribeAttributes());
        attributes.addAll(Arrays.asList(queueAttr));
        return attributes;
    }

    private Collection<String> addRequiredSendAttributes(String[] queueAttr) {
        Set<String> attributes = new HashSet<>(requiredSendAttributes());
        attributes.addAll(Arrays.asList(queueAttr));
        return attributes;
    }

    private MessageRouterConfiguration getConfiguration() {
        MessageRouterConfiguration messageRouterConfiguration = configuration.get();
        if (messageRouterConfiguration == null) {
            throw new IllegalStateException("Router is not initialized");
        }

        return messageRouterConfiguration;
    }
}

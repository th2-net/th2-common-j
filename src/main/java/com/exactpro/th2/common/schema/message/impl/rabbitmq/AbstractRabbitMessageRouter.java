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

import com.exactpro.th2.common.schema.exception.RouterException;
import com.exactpro.th2.common.schema.filter.strategy.FilterStrategy;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageQueue;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.MessageRouterContext;
import com.exactpro.th2.common.schema.message.MessageRouterMonitor;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.exactpro.th2.common.schema.message.impl.context.DefaultMessageRouterContext;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.google.protobuf.Message;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public abstract class AbstractRabbitMessageRouter<T> implements MessageRouter<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitMessageRouter.class);

    private final AtomicReference<MessageRouterContext> context = new AtomicReference<>();
    private final AtomicReference<FilterStrategy<Message>> filterStrategy = new AtomicReference<>(getDefaultFilterStrategy());
    private final ConcurrentMap<String, MessageQueue<T>> queueConnections = new ConcurrentHashMap<>();

    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull MessageRouterConfiguration configuration) {
        Objects.requireNonNull(connectionManager, "Connection owner can not be null");
        Objects.requireNonNull(configuration, "Configuration cannot be null");

        init(new DefaultMessageRouterContext(connectionManager, MessageRouterMonitor.DEFAULT_MONITOR, configuration));
    }

    @Override
    public void init(@NotNull MessageRouterContext context) {
        Objects.requireNonNull(context, "Context can not be null");

        this.context.updateAndGet(prev -> {
            if (prev == null) {
                return context;
            } else {
                throw new IllegalStateException("Router is already initialized");
            }
        });
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
        var filteredByAttrAndFilter = findQueueByFilter(requiredSendAttributes().size() > 0 ? getConfiguration().findQueuesByAttr(requiredSendAttributes()) : getConfiguration().getQueues(), message);

        if (filteredByAttrAndFilter.size() != 1) {
            throw new IllegalStateException("Wrong count of queues for send. Should be equal to 1. Find queues = " + filteredByAttrAndFilter.size());
        }

        send(filteredByAttrAndFilter);
    }

    @Override
    public void send(T message, String... queueAttr) throws IOException {

        var filteredByAttr = getConfiguration().findQueuesByAttr(addRequiredSendAttributes(queueAttr));

        var filteredByAttrAndFilter = findQueueByFilter(filteredByAttr, message);

        if (filteredByAttrAndFilter.size() != 1) {
            throw new IllegalStateException("Wrong size of queues for send. Should be equal to 1");
        }

        send(filteredByAttrAndFilter);
    }

    @Override
    public void sendAll(T message, String... queueAttr) throws IOException {

        var filteredByAttr = getConfiguration().findQueuesByAttr(addRequiredSendAttributes(queueAttr));

        var filteredByAttrAndFilter = findQueueByFilter(filteredByAttr, message);

        if (filteredByAttrAndFilter.isEmpty()) {
            throw new IllegalStateException("Wrong size of queues for send. Can't be equal to 0");
        }

        send(filteredByAttrAndFilter);
    }

    /**
     * Return a fields filter strategy
     * @return filter strategy for filtering message fields
     */
    @NotNull
    public FilterStrategy getFilterStrategy() {
        return this.filterStrategy.get();
    }

    @Override
    public void close() {
        LOGGER.info("Closing message router");

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

        LOGGER.info("Message router has been successfully closed");
    }

    protected abstract MessageQueue<T> createQueue(@NotNull ConnectionManager connectionManager, @NotNull QueueConfiguration queueConfiguration, @NotNull FilterFunction filterFunction);

    protected abstract Map<String, T> findQueueByFilter(Map<String, QueueConfiguration> queues, T msg);

    protected abstract Set<String> requiredSubscribeAttributes();

    protected abstract Set<String> requiredSendAttributes();

    @NotNull
    protected FilterStrategy<Message> getDefaultFilterStrategy() {
        return FilterStrategy.DEFAULT_FILTER_STRATEGY;
    }

    protected MessageRouterMonitor getMonitor() {
        return getContext().getRouterMonitor();
    }

    protected MessageQueue<T> getMessageQueue(String queueAlias) {
        return queueConnections.computeIfAbsent(queueAlias, key -> {
            ConnectionManager connectionManager = getConnectionManager();

            QueueConfiguration queueByAlias = getConfiguration().getQueueByAlias(key);
            if (queueByAlias == null) {
                throw new IllegalStateException("Can not find queue");
            }

            return createQueue(connectionManager, queueByAlias, this::filterMessage);
        });
    }

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

    protected boolean filterMessage(Message msg, List<? extends RouterFilter> filters) {
        return filterStrategy.get().verify(msg, filters);
    }

    @NotNull
    private ConnectionManager getConnectionManager() {
        return getContext().getConnectionManager();
    }

    @NotNull
    private MessageRouterConfiguration getConfiguration() {
        return getContext().getConfiguration();
    }

    @NotNull
    private MessageRouterContext getContext() {
        MessageRouterContext context = this.context.get();
        if (context == null) {
            throw new IllegalStateException("Router is not initialized");
        }
        return context;
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

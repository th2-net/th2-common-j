/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.common.schema.message.impl.rabbitmq;

import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.google.protobuf.Message;
import com.rabbitmq.client.Delivery;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.Histogram.Timer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static com.exactpro.th2.common.schema.message.FilterFunction.DEFAULT_FILTER_FUNCTION;

public abstract class AbstractRabbitSubscriber<T> implements MessageSubscriber<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSubscriber.class);

    private final List<MessageListener<T>> listeners = new CopyOnWriteArrayList<>();
    private final AtomicReference<SubscribeTarget> subscribeTarget = new AtomicReference<>();
    private final AtomicReference<ConnectionManager> connectionManager = new AtomicReference<>();
    private final AtomicReference<SubscriberMonitor> consumerMonitor = new AtomicReference<>();
    private final AtomicReference<FilterFunction> filterFunc = new AtomicReference<>();

    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull SubscribeTarget subscribeTarget) {
        this.init(connectionManager, new SubscribeTarget(subscribeTarget.getQueue(), subscribeTarget.getRoutingKey(), exchangeName), DEFAULT_FILTER_FUNCTION);
    }

    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull SubscribeTarget subscribeTarget, @NotNull FilterFunction filterFunc) {
        Objects.requireNonNull(connectionManager, "Connection can not be null");
        Objects.requireNonNull(subscribeTarget, "Subscriber target can not be null");
        Objects.requireNonNull(filterFunc, "Filter function can not be null");


        if (this.connectionManager.get() != null || this.subscribeTarget.get() != null || this.filterFunc.get() != null) {
            throw new IllegalStateException("Subscriber is already initialize");
        }

        this.connectionManager.set(connectionManager);
        this.subscribeTarget.set(subscribeTarget);
        this.filterFunc.set(filterFunc);
    }

    @Override
    public void start() throws Exception {
        ConnectionManager connectionManager = this.connectionManager.get();
        SubscribeTarget target = subscribeTarget.get();

        if (connectionManager == null || target == null) {
            throw new IllegalStateException("Subscriber is not initialized");
        }

        try {
            var queue = target.getQueue();
            var routingKey = target.getRoutingKey();
            var exchangeName = target.getExchange();

            consumerMonitor.updateAndGet(monitor -> {
                if (monitor == null) {
                    try {
                        monitor = connectionManager.basicConsume(queue, this::handle, this::canceled);
                        LOGGER.info("Start listening exchangeName='{}', routing key='{}', queue name='{}'", exchangeName, routingKey, queue);
                    } catch (IOException e) {
                        throw new IllegalStateException("Can not start subscribe to queue = " + queue, e);
                    }
                }

                return monitor;
            });
        } catch (Exception e) {
            throw new IOException("Can not start listening", e);
        }
    }

    @Override
    public void addListener(MessageListener<T> messageListener) {
        listeners.add(messageListener);
    }

    @Override
    public void close() throws Exception {
        ConnectionManager connectionManager = this.connectionManager.get();
        if (connectionManager == null) {
            throw new IllegalStateException("Subscriber is not initialized");
        }

        SubscriberMonitor monitor = consumerMonitor.getAndSet(null);
        if (monitor != null) {
            monitor.unsubscribe();
        }

        listeners.forEach(MessageListener::onClose);
        listeners.clear();
    }

    protected abstract List<T> valueFromBytes(byte[] body) throws Exception;

    protected abstract String toShortDebugString(T value);

    @Nullable
    protected abstract T filter(T value) throws Exception;

    protected abstract Counter getDeliveryCounter();

    protected abstract Counter getContentCounter();

    protected abstract Histogram getProcessingTimer();

    protected abstract String[] extractLabels(T batch);

    protected abstract int extractCountFrom(T batch);

    private void handle(String consumeTag, Delivery delivery) {
        Timer processTimer = getProcessingTimer().startTimer();

        try {
            List<T> values = valueFromBytes(delivery.getBody());

            for (T value : values) {
                Objects.requireNonNull(value, "Received value is null");

                Counter counter = getDeliveryCounter();
                counter.labels(extractLabels(value)).inc();
                Counter contentCounter = getContentCounter();
                contentCounter.labels(extractLabels(value)).inc(extractCountFrom(value));

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("The received message {}", toShortDebugString(value));
                }

                var filteredValue = filter(value);

                if (Objects.isNull(filteredValue)) {
                    LOGGER.debug("Message is filtered");
                    return;
                }

                for (MessageListener<T> listener : listeners) {
                    try {
                        listener.handler(consumeTag, filteredValue);
                    } catch (Exception listenerExc) {
                        LOGGER.warn("Message listener from class '{}' threw exception", listener.getClass(), listenerExc);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Can not parse value from delivery for: {}", consumeTag, e);
        } finally {
            processTimer.observeDuration();
        }
    }

    protected boolean callFilterFunction(Message message, List<? extends RouterFilter> filters) {
        FilterFunction filterFunction = this.filterFunc.get();
        if (filterFunction == null) {
            throw new IllegalStateException("Subscriber is not initialized");
        }

        return filterFunction.apply(message, filters);
    }

    private void canceled(String consumerTag) {
        LOGGER.warn("Consuming cancelled for: '{}'", consumerTag);
        try {
            close();
        } catch (Exception e) {
            SubscribeTarget subscribeTarget = this.subscribeTarget.get();
            if (subscribeTarget != null) {
                LOGGER.error("Can not close subscriber with exchange name '{}' and queue '{}'", subscribeTarget.getExchange(), subscribeTarget.getQueue(), e);
            } else {
                LOGGER.error("Can not close subscriber without subscribe target", e);
            }
        }
    }

}

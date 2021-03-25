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

import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageRouterContext;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.google.protobuf.Message;
import com.rabbitmq.client.Delivery;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractRabbitSubscriber<T> implements MessageSubscriber<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSubscriber.class);

    private final SubscribeTarget target;
    private final ConnectionManager connectionManager;
    private final FilterFunction filterFunction;
    private final AtomicReference<SubscriberMonitor> consumerMonitor = new AtomicReference<>();
    private final List<MessageListener<T>> listeners = new CopyOnWriteArrayList<>();

    private final HealthMetrics healthMetrics = new HealthMetrics(this);

    public AbstractRabbitSubscriber(@NotNull MessageRouterContext context, @NotNull SubscribeTarget target, @Nullable FilterFunction filterFunction) {
        this.target = Objects.requireNonNull(target, "Subscriber target is can not be null");;
        this.connectionManager = Objects.requireNonNull(context, "Router context can not be null").getConnectionManager();
        this.filterFunction = ObjectUtils.defaultIfNull(filterFunction, FilterFunction.DEFAULT_FILTER_FUNCTION);

        LOGGER.debug("{}:{} created with target {}", getClass().getSimpleName(), hashCode(), target);
    }

    @Override
    public void start() {
        var queue = target.getQueue();
        var routingKey = target.getRoutingKey();
        var exchangeName = target.getExchange();

        consumerMonitor.updateAndGet(monitor -> {
            if (monitor == null) {
                monitor = connectionManager.basicConsume(queue, this::handle, this::canceled, healthMetrics);
                LOGGER.info("Start listening exchangeName='{}', routing key='{}', queue name='{}'", exchangeName, routingKey, queue);
            }
            return monitor;
        });
    }

    @Override
    public void addListener(MessageListener<T> messageListener) {
        listeners.add(messageListener);
    }

    @Override
    public void close() throws Exception {
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

    protected abstract int extractCountFrom(T message);

    private void handle(String consumeTag, Delivery delivery) {
        Histogram.Timer processTimer = getProcessingTimer().startTimer();

        try {
            List<T> values = valueFromBytes(delivery.getBody());

            for (T value : values) {
                Objects.requireNonNull(value, "Received value is null");

                Counter counter = getDeliveryCounter();
                counter.inc();
                Counter contentCounter = getContentCounter();
                contentCounter.inc(extractCountFrom(value));

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("The received message {}", toShortDebugString(value));
                }

                var filteredValue = filter(value);

                if (Objects.isNull(filteredValue)) {
                    LOGGER.debug("Message is filtred");
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

    private void resubscribe() {
        var queue = target.getQueue();
        var routingKey = target.getRoutingKey();
        var exchangeName = target.getExchange();
        LOGGER.info("Try to resubscribe subscriber for exchangeName='{}', routing key='{}', queue name='{}'", exchangeName, routingKey, queue);

        SubscriberMonitor monitor = consumerMonitor.getAndSet(null);
        if (monitor != null) {
            try {
                monitor.unsubscribe();
            } catch (Exception e) {
                LOGGER.info("Can not unsubscribe on resubscribe for exchangeName='{}', routing key='{}', queue name='{}'", exchangeName, routingKey, queue);
            }
        }

        start();
    }

    protected boolean callFilterFunction(Message message, List<? extends RouterFilter> filters) {
        return filterFunction.apply(message, filters);
    }

    private void canceled(String consumerTag) {
        LOGGER.warn("Consuming cancelled for: '{}'", consumerTag);
        healthMetrics.getReadinessMonitor().disable();
        resubscribe();
    }

}

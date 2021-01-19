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

package com.exactpro.th2.common.schema.message.impl.rabbitmq;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.metrics.MetricArbiter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.rabbitmq.client.Delivery;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Gauge.Timer;

public abstract class AbstractRabbitSubscriber<T> implements MessageSubscriber<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSubscriber.class);

    private final List<MessageListener<T>> listeners = new CopyOnWriteArrayList<>();
    private final AtomicReference<String> exchangeName = new AtomicReference<>();
    private final AtomicReference<SubscribeTarget> subscribeTarget = new AtomicReference<>();
    private final AtomicReference<ConnectionManager> connectionManager = new AtomicReference<>();
    private final AtomicReference<SubscriberMonitor> consumerMonitor = new AtomicReference<>();

    private final MetricArbiter livenessArbiter = CommonMetrics.LIVENESS_ARBITER;
    private final MetricArbiter readinessArbiter = CommonMetrics.READINESS_ARBITER;
    private MetricArbiter.MetricMonitor livenessMonitor;
    private MetricArbiter.MetricMonitor readinessMonitor;

    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull SubscribeTarget subscribeTarget) {
        Objects.requireNonNull(connectionManager, "Connection cannot be null");
        Objects.requireNonNull(subscribeTarget, "Subscriber target is can not be null");
        Objects.requireNonNull(exchangeName, "Exchange name in RabbitMQ can not be null");


        if (this.connectionManager.get() != null || this.exchangeName.get() != null || this.subscribeTarget.get() != null) {
            throw new IllegalStateException("Subscriber is already initialize");
        }

        this.connectionManager.set(connectionManager);
        this.subscribeTarget.set(subscribeTarget);
        this.exchangeName.set(exchangeName);

        livenessMonitor = livenessArbiter.register("rabbit_subscriber_liveness");
        readinessMonitor = readinessArbiter.register("rabbit_subscriber_readiness");
    }

    @Override
    public void start() throws Exception {
        ConnectionManager connectionManager = this.connectionManager.get();
        SubscribeTarget target = subscribeTarget.get();
        String exchange = exchangeName.get();

        if (connectionManager == null || target == null || exchange == null) {
            throw new IllegalStateException("Subscriber is not initialized");
        }

        try {
            var queue = target.getQueue();
            var routingKey = target.getRoutingKey();

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

    protected abstract T valueFromBytes(byte[] body) throws Exception;

    protected abstract String toShortDebugString(T value);

    @Nullable
    protected abstract T filter(T value) throws Exception;

    protected abstract Counter getDeliveryCounter();

    protected abstract Counter getContentCounter();

    protected abstract Gauge getProcessingTimer();

    protected abstract int extractCountFrom(T message);

    private void handle(String consumeTag, Delivery delivery) {
        Timer processTimer = getProcessingTimer().startTimer();

        try {
            T value = valueFromBytes(delivery.getBody());

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

        } catch (Exception e) {
            LOGGER.error("Can not parse value from delivery for: {}", consumeTag, e);
        } finally {
            processTimer.setDuration();
        }
    }

    private boolean resubscribe() {
        LOGGER.info("Trying to resubscribe to queue.");

        RabbitMQConfiguration configuration = this.connectionManager.get().getConfiguration();
        int attemptsCount = 0;
        int amountOfAttempts = configuration.getMaxRecoveryAttempts();
        boolean successful = false;

        while (attemptsCount < amountOfAttempts) {
            try {
                Thread.sleep(configuration.getMaxConnectionRecoveryTimeout());
            } catch (InterruptedException | IllegalArgumentException e) {
                LOGGER.error(e.getMessage());
            }

            try {
                attemptsCount++;
                consumerMonitor.set(null);
                start();

                successful = true;

                break;
            } catch (Exception e) {
                if(attemptsCount >= amountOfAttempts) {
                    LOGGER.error("Failed to start listening when resubscribing after {} attempts.", amountOfAttempts);
                }
            }
        }

        if(successful) {
            readinessMonitor.unregister(readinessMonitor.getId());
            livenessMonitor.unregister(livenessMonitor.getId());
        } else {
            if(livenessMonitor.isEnabled()) {
                livenessMonitor.disable();
            }
        }

        return successful;
    }

    private void canceled(String consumerTag) {
        LOGGER.warn("Consuming cancelled for: '{}'", consumerTag);

        readinessMonitor.disable();

        while (true) {
            if (resubscribe()) {
                break;
            }
        }
    }

}

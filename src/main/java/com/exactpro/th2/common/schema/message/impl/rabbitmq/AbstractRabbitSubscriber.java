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

import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.schema.message.ConfirmationMessageListener;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.exactpro.th2.common.metrics.CommonMetrics.DEFAULT_BUCKETS;
import static com.exactpro.th2.common.metrics.CommonMetrics.QUEUE_LABEL;
import static com.exactpro.th2.common.metrics.CommonMetrics.TH2_PIN_LABEL;
import static com.exactpro.th2.common.metrics.CommonMetrics.TH2_TYPE_LABEL;
import static java.util.Objects.requireNonNull;

public abstract class AbstractRabbitSubscriber<T> implements MessageSubscriber<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSubscriber.class);

    private static final Counter MESSAGE_SIZE_SUBSCRIBE_BYTES = Counter.build()
            .name("th2_rabbitmq_message_size_subscribe_bytes")
            .labelNames(TH2_PIN_LABEL, TH2_TYPE_LABEL, QUEUE_LABEL)
            .help("Number of bytes received from RabbitMQ, it includes bytes of messages dropped after filters. " +
                    "For information about the number of dropped messages, please refer to 'th2_message_dropped_subscribe_total' and 'th2_message_group_dropped_subscribe_total'. " +
                    "The message is meant for any data transferred via RabbitMQ, for example, th2 batch message or event or custom content")
            .register();

    private static final Histogram MESSAGE_PROCESS_DURATION_SECONDS = Histogram.build()
            .buckets(DEFAULT_BUCKETS)
            .name("th2_rabbitmq_message_process_duration_seconds")
            .labelNames(TH2_PIN_LABEL, TH2_TYPE_LABEL, QUEUE_LABEL)
            .help("Time of message processing during subscription from RabbitMQ in seconds. " +
                    "The message is meant for any data transferred via RabbitMQ, for example, th2 batch message or event or custom content")
            .register();

    protected final String th2Pin;
    private final List<ConfirmationMessageListener<T>> listeners = new CopyOnWriteArrayList<>();
    private final String queue;
    private final AtomicReference<ConnectionManager> connectionManager = new AtomicReference<>();
    private final AtomicReference<SubscriberMonitor> consumerMonitor = new AtomicReference<>();
    private final AtomicReference<FilterFunction> filterFunc = new AtomicReference<>();
    private final String th2Type;
    private final AtomicBoolean hasManualSubscriber = new AtomicBoolean();

    private final HealthMetrics healthMetrics = new HealthMetrics(this);

    public AbstractRabbitSubscriber(
            @NotNull ConnectionManager connectionManager,
            @NotNull String queue,
            @NotNull FilterFunction filterFunc,
            @NotNull String th2Pin,
            @NotNull String th2Type
    ) {
        this.connectionManager.set(requireNonNull(connectionManager, "Connection can not be null"));
        this.queue = requireNonNull(queue, "Queue can not be null");
        this.filterFunc.set(requireNonNull(filterFunc, "Filter function can not be null"));
        this.th2Pin = requireNonNull(th2Pin, "TH2 pin can not be null");
        this.th2Type = requireNonNull(th2Type, "TH2 type can not be null");
    }

    @Deprecated
    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull SubscribeTarget subscribeTargets) {
        throw new UnsupportedOperationException("Method is deprecated, please use constructor");
    }

    @Deprecated
    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull SubscribeTarget subscribeTarget, @NotNull FilterFunction filterFunc) {
        throw new UnsupportedOperationException("Method is deprecated, please use constructor");
    }

    @Override
    public void start() throws Exception {
        ConnectionManager connectionManager = this.connectionManager.get();
        if (connectionManager == null) {
            throw new IllegalStateException("Subscriber is not initialized");
        }

        try {
            consumerMonitor.updateAndGet(monitor -> {
                if (monitor == null) {
                    try {
                        monitor = connectionManager.basicConsume(
                                queue,
                                (deliveryMetadata, delivery, confirmProcessed) -> {
                                    Timer processTimer = MESSAGE_PROCESS_DURATION_SECONDS
                                            .labels(th2Pin, th2Type, queue)
                                            .startTimer();
                                    MESSAGE_SIZE_SUBSCRIBE_BYTES
                                            .labels(th2Pin, th2Type, queue)
                                            .inc(delivery.getBody().length);
                                    try {
                                        T value;
                                        try {
                                            value = valueFromBytes(delivery.getBody());
                                        } catch (Exception e) {
                                            LOGGER.error("Couldn't parse delivery. Reject message received", e);
                                            confirmProcessed.reject();
                                            throw new IOException(
                                                    String.format(
                                                            "Can not extract value from bytes for envelope '%s', queue '%s', pin '%s'",
                                                            delivery.getEnvelope(), queue, th2Pin
                                                    ),
                                                    e
                                            );
                                        }
                                        handle(deliveryMetadata.getConsumerTag(), delivery, value, confirmProcessed);
                                    } finally {
                                        processTimer.observeDuration();
                                    }
                                },
                                this::canceled
                        );
                        LOGGER.info("Start listening queue name='{}'", queue);
                    } catch (IOException e) {
                        throw new IllegalStateException("Can not start subscribe to queue = " + queue, e);
                    }
                }

                return monitor;
            });
        } catch (Exception e) {
            throw new IllegalStateException("Can not start listening", e);
        }
    }

    @Override
    public void addListener(ConfirmationMessageListener<T> messageListener) {
        if (ConfirmationMessageListener.isManual(messageListener)) {
            if (!hasManualSubscriber.compareAndSet(false, true)) {
                throw new IllegalStateException("cannot subscribe listener " + messageListener
                        + " because only one listener with manual confirmation is allowed per queue");
            }
        }
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

        listeners.forEach(ConfirmationMessageListener::onClose);
        listeners.clear();
    }

    protected boolean callFilterFunction(Message message, List<? extends RouterFilter> filters) {
        FilterFunction filterFunction = this.filterFunc.get();
        if (filterFunction == null) {
            throw new IllegalStateException("Subscriber is not initialized");
        }

        return filterFunction.apply(message, filters);
    }

    protected abstract T valueFromBytes(byte[] body) throws Exception;

    protected abstract String toShortTraceString(T value);

    protected abstract String toShortDebugString(T value);

    @Nullable
    protected abstract T filter(T value) throws Exception;

    protected void handle(String consumeTag, Delivery delivery, T value, Confirmation confirmation) throws IOException {
        try {
            String routingKey = delivery.getEnvelope().getRoutingKey();
            requireNonNull(value, () -> "Received value from " + routingKey + " is null");

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Received message from {}: {}", routingKey, toShortTraceString(value));
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Received message from {}: {}", routingKey, toShortDebugString(value));
            }

            var filteredValue = filter(value);

            if (Objects.isNull(filteredValue)) {
                LOGGER.debug("Message is filtered");
                confirmation.confirm();
                return;
            }

            boolean hasManualConfirmation = false;
            for (ConfirmationMessageListener<T> listener : listeners) {
                try {
                    boolean redeliver = delivery.getEnvelope().isRedeliver();
                    listener.handle(new DeliveryMetadata(consumeTag, redeliver), filteredValue, confirmation);
                    if (!hasManualConfirmation) {
                        hasManualConfirmation = ConfirmationMessageListener.isManual(listener);
                    }
                } catch (Exception listenerExc) {
                    LOGGER.warn("Message listener from class '{}' threw exception", listener.getClass(), listenerExc);
                }
            }
            if (!hasManualConfirmation) {
                confirmation.confirm();
            }
        } catch (Exception e) {
            LOGGER.error("Can not parse value from delivery for: {}. Reject message received", consumeTag, e);
            confirmation.reject();
        }
    }

    private void resubscribe() {
        LOGGER.info("Try to resubscribe subscriber for queue name='{}'", queue);

        SubscriberMonitor monitor = consumerMonitor.getAndSet(null);
        if (monitor != null) {
            try {
                monitor.unsubscribe();
            } catch (Exception e) {
                LOGGER.info("Can not unsubscribe on resubscribe for queue name='{}'", queue);
            }
        }

        try {
            start();
        } catch (Exception e) {
            LOGGER.error("Can not resubscribe subscriber for queue name='{}'", queue);
            healthMetrics.disable();
        }
    }

    private void canceled(String consumerTag) {
        LOGGER.warn("Consuming cancelled for: '{}'", consumerTag);
        healthMetrics.getReadinessMonitor().disable();
        resubscribe();
    }
}

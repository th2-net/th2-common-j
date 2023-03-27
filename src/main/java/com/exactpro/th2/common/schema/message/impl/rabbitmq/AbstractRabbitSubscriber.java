/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.schema.message.ConfirmationListener;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.google.common.base.Suppliers;
import com.rabbitmq.client.Delivery;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.Histogram.Timer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.exactpro.th2.common.metrics.CommonMetrics.DEFAULT_BUCKETS;
import static com.exactpro.th2.common.metrics.CommonMetrics.QUEUE_LABEL;
import static com.exactpro.th2.common.metrics.CommonMetrics.TH2_PIN_LABEL;
import static com.exactpro.th2.common.metrics.CommonMetrics.TH2_TYPE_LABEL;
import static java.util.Objects.requireNonNull;

public abstract class AbstractRabbitSubscriber<T> implements MessageSubscriber<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSubscriber.class);

    @SuppressWarnings("rawtypes")
    private static final Supplier EMPTY_INITIALIZER = Suppliers.memoize(() -> null);

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

    protected final ReentrantLock publicLock = new ReentrantLock();
    private boolean hasManualSubscriber = false;

    protected final String th2Pin;
    private final Set<ConfirmationListener<T>> listeners = ConcurrentHashMap.newKeySet();
    private final String queue;
    private final ConnectionManager connectionManager;
    private final AtomicReference<Supplier<SubscriberMonitor>> consumerMonitor = new AtomicReference<>(emptySupplier());
    private final String th2Type;

    private final HealthMetrics healthMetrics = new HealthMetrics(this);

    public AbstractRabbitSubscriber(
            @NotNull ConnectionManager connectionManager,
            @NotNull String queue,
            @NotNull String th2Pin,
            @NotNull String th2Type
    ) {
        this.connectionManager = requireNonNull(connectionManager, "Connection can not be null");
        this.queue = requireNonNull(queue, "Queue can not be null");
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
    @Deprecated
    public void start() throws Exception {
        // Do nothing
    }

    @Override
    public void addListener(ConfirmationListener<T> messageListener) {
        publicLock.lock();
        try {
            boolean isManual = ConfirmationListener.isManual(messageListener);
            if (isManual && hasManualSubscriber) {
                throw new IllegalStateException("cannot subscribe listener " + messageListener
                        + " because only one listener with manual confirmation is allowed per queue");
            }
            if(listeners.add(messageListener)) {
                hasManualSubscriber |= isManual;
                subscribe();
            }
        } finally {
            publicLock.unlock();
        }
    }

    @Override
    public void removeListener(ConfirmationListener<T> messageListener) {
        publicLock.lock();
        try {
            boolean isManual = ConfirmationListener.isManual(messageListener);
            if (listeners.remove(messageListener)) {
                hasManualSubscriber &= !isManual;
                messageListener.onClose();
            }
        } finally {
            publicLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        publicLock.lock();
        try {
            SubscriberMonitor monitor = consumerMonitor.getAndSet(emptySupplier()).get();
            if (monitor != null) {
                monitor.unsubscribe();
            }

            Iterator<ConfirmationListener<T>> listIterator = listeners.iterator();
            while (listIterator.hasNext()) {
                ConfirmationListener<T> listener = listIterator.next();
                listIterator.remove();
                listener.onClose();
            }
        } finally {
            publicLock.unlock();
        }
    }

    protected abstract T valueFromBytes(byte[] body) throws Exception;

    protected abstract String toShortTraceString(T value);

    protected abstract String toShortDebugString(T value);

    @Nullable
    protected abstract T filter(T value) throws Exception;

    protected void handle(DeliveryMetadata deliveryMetadata, Delivery delivery, T value, Confirmation confirmation) throws IOException {
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
            for (ConfirmationListener<T> listener : listeners) {
                try {
                    listener.handle(deliveryMetadata, filteredValue, confirmation);
                    if (!hasManualConfirmation) {
                        hasManualConfirmation = ConfirmationListener.isManual(listener);
                    }
                } catch (Exception listenerExc) {
                    LOGGER.warn("Message listener from class '{}' threw exception", listener.getClass(), listenerExc);
                }
            }
            if (!hasManualConfirmation) {
                confirmation.confirm();
            }
        } catch (Exception e) {
            LOGGER.error("Can not parse value from delivery for: {}. Reject message received", deliveryMetadata, e);
            confirmation.reject();
        }
    }

    private void subscribe() {
        try {
            consumerMonitor.updateAndGet(previous -> previous == EMPTY_INITIALIZER
                            ? Suppliers.memoize(this::basicConsume)
                            : previous)
                    .get(); // initialize subscribtion
        } catch (Exception e) {
            throw new IllegalStateException("Can not start listening", e);
        }
    }

    private void resubscribe() {
        LOGGER.info("Try to resubscribe subscriber for queue name='{}'", queue);

        SubscriberMonitor monitor = consumerMonitor.getAndSet(emptySupplier()).get();
        if (monitor != null) {
            try {
                monitor.unsubscribe();
            } catch (Exception e) {
                LOGGER.info("Can not unsubscribe on resubscribe for queue name='{}'", queue);
            }
        }

        try {
            subscribe();
        } catch (Exception e) {
            LOGGER.error("Can not resubscribe subscriber for queue name='{}'", queue);
            healthMetrics.disable();
        }
    }

    private SubscriberMonitor basicConsume() {
        try {
            LOGGER.info("Start listening queue name='{}', th2 pin='{}'", queue, th2Pin);
            return connectionManager.basicConsume(queue, this::handle, this::canceled);
        } catch (IOException e) {
            throw new IllegalStateException("Can not subscribe to queue = " + queue, e);
        }
    }

    private void handle(DeliveryMetadata deliveryMetadata,
                        Delivery delivery,
                        Confirmation confirmProcessed) throws IOException {
        try (Timer ignored = MESSAGE_PROCESS_DURATION_SECONDS
                .labels(th2Pin, th2Type, queue)
                .startTimer()) {
            MESSAGE_SIZE_SUBSCRIBE_BYTES
                    .labels(th2Pin, th2Type, queue)
                    .inc(delivery.getBody().length);

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
            handle(deliveryMetadata, delivery, value, confirmProcessed);
        }
    }

    private void canceled(String consumerTag) {
        LOGGER.warn("Consuming cancelled for: '{}'", consumerTag);
        healthMetrics.getReadinessMonitor().disable();
        resubscribe();
    }

    @SuppressWarnings("unchecked")
    private static <T> Supplier<T> emptySupplier() {
        return (Supplier<T>) EMPTY_INITIALIZER;
    }
}

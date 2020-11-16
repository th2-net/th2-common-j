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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventBatchOrBuilder;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageBatchOrBuilder;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.grpc.RawMessageBatchOrBuilder;
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

    private static final Counter INCOMING_PARSED_MSG_BATCH_QUANTITY = Counter.build("th2_mq_incoming_parsed_batch_msg_quantity", "Quantity of incoming parsed message batches").register();
    private static final Counter INCOMING_PARSED_MSG_QUANTITY = Counter.build("th2_mq_incoming_parsed_msg_quantity", "Quantity of incoming parsed messages").register();
    private static final Counter INCOMING_RAW_MSG_BATCH_QUANTITY = Counter.build("th2_mq_incoming_raw_batch_msg_quantity", "Quantity of incoming raw message batches").register();
    private static final Counter INCOMING_RAW_MSG_QUANTITY = Counter.build("th2_mq_incoming_raw_msg_quantity", "Quantity of incoming raw messages").register();
    private static final Counter INCOMING_EVENT_BATCH_QUANTITY = Counter.build("th2_mq_incoming_event_batch_quantity", "Quantity of incoming event batches").register();
    private static final Counter INCOMING_EVENT_QUANTITY = Counter.build("th2_mq_incoming_event_quantity", "Quantity of incoming events").register();

    private static final Gauge PARSED_MSG_PROCESSING_TIME = Gauge.build("th2_mq_parsed_msg_processing_time", "Time of processing parsed messages").register();
    private static final Gauge RAW_MSG_PROCESSING_TIME = Gauge.build("th2_mq_raw_msg_processing_time", "Time of processing raw messages").register();
    private static final Gauge EVENT_PROCESSING_TIME = Gauge.build("th2_mq_event_processing_time", "Time of processing events").register();

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


    private void handle(String consumeTag, Delivery delivery) {
        try {
            T value = valueFromBytes(delivery.getBody());

            Timer processTimer = null;

            if (value instanceof MessageBatch) {
                INCOMING_PARSED_MSG_BATCH_QUANTITY.inc();
                INCOMING_PARSED_MSG_QUANTITY.inc(((MessageBatchOrBuilder)value).getMessagesCount());
                processTimer = PARSED_MSG_PROCESSING_TIME.startTimer();
            }

            if (value instanceof RawMessageBatch) {
                INCOMING_RAW_MSG_BATCH_QUANTITY.inc();
                INCOMING_RAW_MSG_QUANTITY.inc(((RawMessageBatchOrBuilder)value).getMessagesCount());
                processTimer = RAW_MSG_PROCESSING_TIME.startTimer();
            }

            if (value instanceof EventBatch) {
                INCOMING_EVENT_BATCH_QUANTITY.inc();
                INCOMING_EVENT_QUANTITY.inc(((EventBatchOrBuilder)value).getEventsCount());
                processTimer = EVENT_PROCESSING_TIME.startTimer();
            }

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

            if (processTimer != null) {
                processTimer.setDuration();
            }

        } catch (Exception e) {
            LOGGER.error("Can not parse value from delivery for: {}", consumeTag, e);
        }
    }

    private void canceled(String consumerTag) {
        LOGGER.warn("Consuming cancelled for: '{}'", consumerTag);
        try {
            close();
        } catch (Exception e) {
            LOGGER.error("Can not close subscriber with exchange name '{}' and queues '{}'", exchangeName.get(), subscribeTarget.get(), e);
        }
    }

}

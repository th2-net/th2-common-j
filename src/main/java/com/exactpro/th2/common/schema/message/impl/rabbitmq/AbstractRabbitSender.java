/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.rabbitmq.http.client.domain.QueueInfo;

import static com.exactpro.th2.common.metrics.CommonMetrics.EXCHANGE_LABEL;
import static com.exactpro.th2.common.metrics.CommonMetrics.ROUTING_KEY_LABEL;
import static com.exactpro.th2.common.metrics.CommonMetrics.TH2_PIN_LABEL;
import static com.exactpro.th2.common.metrics.CommonMetrics.TH2_TYPE_LABEL;
import io.prometheus.client.Counter;
import static java.util.Objects.requireNonNull;

public abstract class AbstractRabbitSender<T> implements MessageSender<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSender.class);

    private static final Counter MESSAGE_SIZE_PUBLISH_BYTES = Counter.build()
            .name("th2_rabbitmq_message_size_publish_bytes")
            .labelNames(TH2_PIN_LABEL, TH2_TYPE_LABEL, EXCHANGE_LABEL, ROUTING_KEY_LABEL)
            .help("Number of published message bytes to RabbitMQ. " +
                    "The message is meant for any data transferred via RabbitMQ, for example, th2 batch message or event or custom content")
            .register();

    private static final Counter MESSAGE_PUBLISH_TOTAL = Counter.build()
            .name("th2_rabbitmq_message_publish_total")
            .labelNames(TH2_PIN_LABEL, TH2_TYPE_LABEL, EXCHANGE_LABEL, ROUTING_KEY_LABEL)
            .help("Quantity of published messages to RabbitMQ. " +
                    "The message is meant for any data transferred via RabbitMQ, for example, th2 batch message or event or custom content")
            .register();
    public static final int MIN_SLEEP_SHIFT = 10;
    public static final int MAX_SLEEP_SHIFT = 100;

    protected final String th2Pin;
    private final AtomicReference<ConnectionManager> connectionManager = new AtomicReference<>();
    private final AtomicReference<String> routingKey = new AtomicReference<>();
    private final AtomicReference<String> exchangeName = new AtomicReference<>();
    private final long messageAmountToCheckVirtualQueueLimit;
    private final long virtualQueueLimit;
    private final int maxIntervalToCheckVirtualQueueLimit;
    private final String th2Type;
    private final AtomicLong totalSent = new AtomicLong(0);

    public AbstractRabbitSender(
            @NotNull ConnectionManager connectionManager,
            @NotNull QueueConfiguration queueConfiguration,
            @NotNull String th2Pin,
            @NotNull String th2Type
    ) {
        this.connectionManager.set(requireNonNull(connectionManager, "Connection manager can not be null"));
        requireNonNull(queueConfiguration, "Queue configuration can not be null");
        exchangeName.set(requireNonNull(queueConfiguration.getExchange(), "Exchange name can not be null"));
        routingKey.set(requireNonNull(queueConfiguration.getRoutingKey(), "Routing key can not be null"));
        if (queueConfiguration.getMessageAmountToCheckVirtualQueueLimit() <= 0) {
            throw new IllegalArgumentException("'messageAmountToCheckVirtualQueueLimit' should be greater than zero, actual: " + queueConfiguration.getMessageAmountToCheckVirtualQueueLimit());
        }
        messageAmountToCheckVirtualQueueLimit = queueConfiguration.getMessageAmountToCheckVirtualQueueLimit();
        if (queueConfiguration.getVirtualQueueLimit() <= 0) {
            throw new IllegalArgumentException("'virtualQueueLimit' should be greater than zero, actual: " + queueConfiguration.getVirtualQueueLimit());
        }
        virtualQueueLimit = queueConfiguration.getVirtualQueueLimit();
        if (queueConfiguration.getMaxIntervalToCheckVirtualQueueLimit() <= 0) {
            throw new IllegalArgumentException("'maxIntervalToCheckVirtualQueueLimit' should be greater than zero, actual: " + queueConfiguration.getMaxIntervalToCheckVirtualQueueLimit());
        }
        maxIntervalToCheckVirtualQueueLimit = queueConfiguration.getMaxIntervalToCheckVirtualQueueLimit();
        this.th2Pin = requireNonNull(th2Pin, "TH2 pin can not be null");
        this.th2Type = requireNonNull(th2Type, "TH2 type can not be null");
    }

    @Deprecated
    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull String routingKey) {
        throw new UnsupportedOperationException("Method is deprecated, please use constructor");
    }

    @Override
    public void send(T value) throws IOException {
        requireNonNull(value, "Value for send can not be null");

        try {
            ConnectionManager connectionManager = this.connectionManager.get();
            if (totalSent.get() == messageAmountToCheckVirtualQueueLimit) {
                List<QueueInfo> exceededQueues = connectionManager.getExceededQueues(exchangeName.get(), routingKey.get(), virtualQueueLimit);
                while (!exceededQueues.isEmpty()) {
                    int sleepInterval = /*Math.exp(?) + */MIN_SLEEP_SHIFT + new Random().nextInt(MAX_SLEEP_SHIFT - MIN_SLEEP_SHIFT + 1);
                    if (sleepInterval > maxIntervalToCheckVirtualQueueLimit) {
                        sleepInterval = maxIntervalToCheckVirtualQueueLimit;
                    }
                    LOGGER.info(
                            "There are {} which is more or equals than size limit = {}, waiting {} seconds before recheck",
                            exceededQueues.stream()
                                    .map(queueInfo -> queueInfo.getTotalMessages() + " message(s) in '" + queueInfo.getName() + "'")
                                    .collect(Collectors.joining(", ")),
                            virtualQueueLimit,
                            sleepInterval
                    );
                    Thread.sleep(sleepInterval * 1000L);
                    exceededQueues = connectionManager.getExceededQueues(exchangeName.get(), routingKey.get(), virtualQueueLimit);
                }
                totalSent.set(0);
            }

            byte[] bytes = valueToBytes(value);
            MESSAGE_SIZE_PUBLISH_BYTES
                    .labels(th2Pin, th2Type, exchangeName.get(), routingKey.get())
                    .inc(bytes.length);
            MESSAGE_PUBLISH_TOTAL
                    .labels(th2Pin, th2Type, exchangeName.get(), routingKey.get())
                    .inc();
            connectionManager.basicPublish(exchangeName.get(), routingKey.get(), null, bytes);
            totalSent.incrementAndGet();

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Message sent to exchangeName='{}', routing key='{}': '{}'",
                        exchangeName, routingKey, toShortTraceString(value));
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Message sent to exchangeName='{}', routing key='{}': '{}'",
                        exchangeName, routingKey, toShortDebugString(value));
            }
        } catch (Exception e) {
            throw new IOException("Can not send message: " + toShortDebugString(value), e);
        }
    }

    protected abstract String toShortTraceString(T value);

    protected abstract String toShortDebugString(T value);

    protected abstract byte[] valueToBytes(T value);
}

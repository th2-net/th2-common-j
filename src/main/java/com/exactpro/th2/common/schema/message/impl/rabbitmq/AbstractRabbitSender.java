/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;

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

    protected final String th2Pin;
    private final AtomicReference<ConnectionManager> connectionManager = new AtomicReference<>();
    private final AtomicReference<String> routingKey = new AtomicReference<>();
    private final AtomicReference<String> exchangeName = new AtomicReference<>();
    private final String queueName;
    private final long sizeLimit;
    private final int recheckSizeTimeoutSeconds;
    private final String th2Type;

    public AbstractRabbitSender(
            @NotNull ConnectionManager connectionManager,
            @NotNull QueueConfiguration queueConfiguration,
            @NotNull String th2Pin,
            @NotNull String th2Type
    ) {
        this.connectionManager.set(requireNonNull(connectionManager, "Connection can not be null"));
        requireNonNull(queueConfiguration, "Queue configuration can not be null");
        exchangeName.set(requireNonNull(queueConfiguration.getExchange(), "Exchange name can not be null"));
        routingKey.set(requireNonNull(queueConfiguration.getRoutingKey(), "Routing key can not be null"));
        queueName = requireNonNull(queueConfiguration.getQueue(), "Queue name can not be null");
        sizeLimit = queueConfiguration.getSizeLimit();
        recheckSizeTimeoutSeconds = queueConfiguration.getRecheckSizeTimeoutSeconds();
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
            long messageCount = connectionManager.getMessageCount(queueName);
            while (messageCount >= sizeLimit) {
                LOGGER.info(
                        "There are {} message(s) in '{}' which is more or equal than size limit = {}, waiting {} seconds before recheck",
                        messageCount,
                        queueName,
                        sizeLimit,
                        recheckSizeTimeoutSeconds
                );
                Thread.sleep(recheckSizeTimeoutSeconds * 1000L);
                messageCount = connectionManager.getMessageCount(queueName);
            }

            byte[] bytes = valueToBytes(value);
            MESSAGE_SIZE_PUBLISH_BYTES
                    .labels(th2Pin, th2Type, exchangeName.get(), routingKey.get())
                    .inc(bytes.length);
            MESSAGE_PUBLISH_TOTAL
                    .labels(th2Pin, th2Type, exchangeName.get(), routingKey.get())
                    .inc();
            connectionManager.basicPublish(exchangeName.get(), routingKey.get(), null, bytes);

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

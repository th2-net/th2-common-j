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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.metrics.MetricArbiter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;

import io.prometheus.client.Counter;

public abstract class AbstractRabbitSender<T> implements MessageSender<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSender.class);

    private final AtomicReference<String> sendQueue = new AtomicReference<>();
    private final AtomicReference<String> exchangeName = new AtomicReference<>();
    private final AtomicReference<ConnectionManager> connectionManager = new AtomicReference<>();

    private final MetricArbiter.MetricMonitor livenessMonitor = CommonMetrics.getLIVENESS_ARBITER().register(getClass().getSimpleName() + "_liveness_" + hashCode());
    private final MetricArbiter.MetricMonitor readinessMonitor = CommonMetrics.getREADINESS_ARBITER().register(getClass().getSimpleName() + "_readiness_" + hashCode());

    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull String sendQueue) {
        Objects.requireNonNull(connectionManager, "Connection can not be null");
        Objects.requireNonNull(exchangeName, "Exchange name can not be null");
        Objects.requireNonNull(sendQueue, "Send queue can not be null");

        if (this.connectionManager.get() != null && this.sendQueue.get() != null && this.exchangeName.get() != null) {
            throw new IllegalStateException("Sender is already initialize");
        }

        this.connectionManager.set(connectionManager);
        this.exchangeName.set(exchangeName);
        this.sendQueue.set(sendQueue);

        LOGGER.debug("{}:{} initialised with queue {}", getClass().getSimpleName(), hashCode(), sendQueue);
    }

    protected abstract Counter getDeliveryCounter();

    protected abstract Counter getContentCounter();

    protected abstract int extractCountFrom(T message);

    @Override
    public void send(T value) throws IOException {
        Objects.requireNonNull(value, "Value for send can not be null");

        Counter counter = getDeliveryCounter();
        counter.inc();
        Counter contentCounter = getContentCounter();
        contentCounter.inc(extractCountFrom(value));

        try {
            ConnectionManager connection = this.connectionManager.get();
            connection.basicPublish(exchangeName.get(), sendQueue.get(), null, valueToBytes(value), readinessMonitor);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Message sent to exchangeName='{}', routing key='{}': '{}'",
                        exchangeName, sendQueue, toShortDebugString(value));
            }
        } catch (Exception e) {
            throw new IOException("Can not send message: " + toShortDebugString(value), e);
        }
    }

    protected String toShortDebugString(T value) {
        return value.toString();
    }

    protected abstract byte[] valueToBytes(T value);


}

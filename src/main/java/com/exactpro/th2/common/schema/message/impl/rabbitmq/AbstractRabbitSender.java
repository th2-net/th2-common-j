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

import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.metrics.MetricArbiter;
import com.exactpro.th2.common.metrics.MetricMonitor;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ResendMessageConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.Resender;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.SendData;
import io.prometheus.client.Counter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class AbstractRabbitSender<T> implements MessageSender<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSender.class);

    private final String sendQueue;
    private final String exchangeName;
    private final Resender resender;
    private final AtomicInteger resendMessages = new AtomicInteger(0);
    private final AtomicBoolean canSend = new AtomicBoolean(true);

    private final MetricMonitor livenessMonitor = CommonMetrics.registerLiveness(this);
    private final MetricMonitor readinessMonitor = CommonMetrics.registerReadiness(this);

    protected AbstractRabbitSender(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull String sendQueue) {
        this.resender = Objects.requireNonNull(connectionManager, "Connection can not be null")
                .createResender(new HealthMetrics(livenessMonitor, readinessMonitor), this::unlock);
        this.exchangeName = Objects.requireNonNull(exchangeName, "Exchange name can not be null");
        this.sendQueue = Objects.requireNonNull(sendQueue, "Send queue can not be null");

        LOGGER.info("{}:{} initialised with queue {}", getClass().getSimpleName(), hashCode(), sendQueue);
    }

    protected abstract Counter getDeliveryCounter();

    protected abstract Counter getContentCounter();

    protected abstract int extractCountFrom(T message);

    @Override
    public void send(T value) {
        Objects.requireNonNull(value, "Value for send can not be null");

        Counter counter = getDeliveryCounter();
        counter.inc();
        Counter contentCounter = getContentCounter();
        contentCounter.inc(extractCountFrom(value));

        while (!canSend.get()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Interrupted on waiting unblock send", e);
            }
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Message try to send to exchangeName='{}', routing key='{}': '{}'",
                    exchangeName, sendQueue, toShortDebugString(value));
        }

        send(exchangeName, sendQueue, value);
    }

    protected abstract String toShortDebugString(T value);

    protected abstract byte[] valueToBytes(T value);

    private void unlock() {
        canSend.set(true);
    }

    private void send(String exchangeName, String routingKey, T message) {
        resender.sendAsync(new SendData(exchangeName, routingKey, null, valueToBytes(message), new DelayHandler(exchangeName, routingKey, message), new SuccessHandler(exchangeName, routingKey, message)));
    }

    private class DelayHandler implements Function<ResendMessageConfiguration, Long> {
        private final String exchange;
        private final String routingKey;
        private final T message;

        public DelayHandler(String exchange, String routingKey, T message) {
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.message = message;
        }

        @Override
        public Long apply(ResendMessageConfiguration configuration) {
            long count = resendMessages.incrementAndGet();
            if (count >= configuration.getCountRejectsToBlockSender()) {
                canSend.set(false);
            }

            LOGGER.warn("Retry send message to exchangeName='{}', routing key='{}': '{}'",
                    exchange, routingKey, toShortDebugString(message));
            long delay = (long) (Math.E * count * configuration.getMinDelay());
            return configuration.getMaxDelay() > 0 ? Math.min(delay, configuration.getMaxDelay()) :  delay;
        }
    }

    private class SuccessHandler implements Consumer<ResendMessageConfiguration> {

        private final String exchange;
        private final String routingKey;
        private final T message;

        public SuccessHandler(String exchange, String routingKey, T message) {
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.message = message;
        }

        @Override
        public void accept(ResendMessageConfiguration configuration) {
            if (resendMessages.getAndSet(0) >= configuration.getCountRejectsToBlockSender()) {
                canSend.set(true);
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Message sent to exchangeName='{}', routing key='{}': '{}'",
                        exchange, routingKey, toShortDebugString(message));
            }
        }
    }

}

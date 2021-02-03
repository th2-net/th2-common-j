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

import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.metrics.MainMetrics;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ResendMessageConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import io.prometheus.client.Counter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public abstract class AbstractRabbitSender<T> implements MessageSender<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSender.class);

    private final AtomicReference<String> sendQueue = new AtomicReference<>();
    private final AtomicReference<String> exchangeName = new AtomicReference<>();
    private final AtomicReference<ConnectionManager> connectionManager = new AtomicReference<>();
    private final AtomicReference<ResendMessageConfiguration> resendConfiguration = new AtomicReference<>();
    private final AtomicInteger resendMessages = new AtomicInteger(0);
    private final AtomicBoolean canSend = new AtomicBoolean(true);

    private final MainMetrics metrics = new MainMetrics(
            CommonMetrics.getLIVENESS_ARBITER().register(getClass().getSimpleName() + "_liveness_" + hashCode()),
            CommonMetrics.getREADINESS_ARBITER().register(getClass().getSimpleName() + "_readiness_" + hashCode())
    );

    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull String sendQueue) {
        Objects.requireNonNull(connectionManager, "Connection can not be null");
        Objects.requireNonNull(exchangeName, "Exchange name can not be null");
        Objects.requireNonNull(sendQueue, "Send queue can not be null");
        Objects.requireNonNull(connectionManager.getResendConfiguration(), "Resend message configuration can not be null");

        if (this.connectionManager.get() != null && this.sendQueue.get() != null && this.exchangeName.get() != null && this.resendConfiguration.get() != null) {
            throw new IllegalStateException("Sender is already initialize");
        }

        this.connectionManager.set(connectionManager);
        this.exchangeName.set(exchangeName);
        this.sendQueue.set(sendQueue);
        this.resendConfiguration.set(connectionManager.getResendConfiguration());

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

            while (!canSend.get()) {
                Thread.sleep(1);
            }

            ConnectionManager connection = this.connectionManager.get();

            if (connection == null) {
                throw new IllegalStateException("Sender is not yet init");
            }

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Message try to send to exchangeName='{}', routing key='{}': '{}'",
                        exchangeName, sendQueue, toShortDebugString(value));
            }

            send(connection, exchangeName.get(), sendQueue.get(), value);

        } catch (Exception e) {
            throw new IOException("Can not send message: " + toShortDebugString(value), e);
        }
    }

    protected String toShortDebugString(T value) {
        return value.toString();
    }

    protected abstract byte[] valueToBytes(T value);

    private void send(ConnectionManager connection, String exchangeName, String routingKey, T message) {
        connection.basicPublish(exchangeName, routingKey, null, valueToBytes(message),
                new DelayHandler(exchangeName, routingKey, message), new SuccessHandler(exchangeName, routingKey, message), metrics);
    }

    private class DelayHandler implements Supplier<Long> {
        private final String exchange;
        private final String routingKey;
        private final T message;
        private final ResendMessageConfiguration resendMessageConfiguration = resendConfiguration.get();

        public DelayHandler(String exchange, String routingKey, T message) {
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.message = message;
        }

        @Override
        public Long get() {
            long count = resendMessages.incrementAndGet();
            if (count >= resendMessageConfiguration.getCountRejectsToBlockSender()) {
                canSend.set(false);
            }

            LOGGER.warn("Retry send message to exchangeName='{}', routing key='{}': '{}'",
                    exchange, routingKey, toShortDebugString(message));
            long delay = (long) (Math.E * count * resendMessageConfiguration.getMinDelay());
            return resendMessageConfiguration.getMaxDelay() > 0 ? Math.min(delay, resendMessageConfiguration.getMaxDelay()) :  delay;
        }
    }

    private class SuccessHandler implements Runnable {

        private final String exchange;
        private final String routingKey;
        private final T message;

        public SuccessHandler(String exchange, String routingKey, T message) {
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.message = message;
        }

        @Override
        public void run() {
            if (resendMessages.getAndSet(0) >= resendConfiguration.get().getCountRejectsToBlockSender()) {
                canSend.set(true);
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Message sent to exchangeName='{}', routing key='{}': '{}'",
                        exchange, routingKey, toShortDebugString(message));
            }
        }
    }

}

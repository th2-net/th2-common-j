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
package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ResendMessageConfiguration;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownNotifier;
import com.rabbitmq.client.TopologyRecoveryException;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

public class ConnectionManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private final Connection connection;
    private final ThreadLocal<Channel> channel = ThreadLocal.withInitial(this::createChannel);
    private final AtomicInteger connectionRecoveryAttempts = new AtomicInteger(0);
    private final AtomicBoolean connectionIsClosed = new AtomicBoolean(false);
    private final RabbitMQConfiguration configuration;
    private final ResendMessageConfiguration resendConfiguration;
    private final String subscriberName;
    private final AtomicInteger nextSubscriberId = new AtomicInteger(1);
    private final ThreadLocal<Map<Long, PublishData>> rejectHandlers = ThreadLocal.withInitial(ConcurrentHashMap::new);
    private final ExecutorService resendRejectService;

    public ConnectionManager(@NotNull RabbitMQConfiguration rabbitMQConfiguration, Runnable onFailedRecoveryConnection) {
        this.configuration = Objects.requireNonNull(rabbitMQConfiguration, "RabbitMQ configuration cannot be null");
        this.resendConfiguration = configuration.getResendMessageConfiguration();

        if (StringUtils.isBlank(rabbitMQConfiguration.getSubscriberName())) {
            subscriberName = "rabbit_mq_subscriber." + System.currentTimeMillis();
            LOGGER.info("Subscribers will use default name: {}", subscriberName);
        } else {
            subscriberName = rabbitMQConfiguration.getSubscriberName() + "." + System.currentTimeMillis();
        }

        val factory = new ConnectionFactory();
        val virtualHost = rabbitMQConfiguration.getVHost();
        val username = rabbitMQConfiguration.getUsername();
        val password = rabbitMQConfiguration.getPassword();

        factory.setHost(rabbitMQConfiguration.getHost());
        factory.setPort(rabbitMQConfiguration.getPort());

        if (StringUtils.isNotBlank(virtualHost)) {
            factory.setVirtualHost(virtualHost);
        }

        if (StringUtils.isNotBlank(username)) {
            factory.setUsername(username);
        }

        if (StringUtils.isNotBlank(password)) {
            factory.setPassword(password);
        }

        if (rabbitMQConfiguration.getConnectionTimeout() > 0) {
            factory.setConnectionTimeout(rabbitMQConfiguration.getConnectionTimeout());
        }

        factory.setExceptionHandler(new ExceptionHandler() {
            @Override
            public void handleUnexpectedConnectionDriverException(Connection conn, Throwable exception) {
                turnOffReandess(exception);
            }

            @Override
            public void handleReturnListenerException(Channel channel, Throwable exception) {
                turnOffReandess(exception);
            }

            @Override
            public void handleConfirmListenerException(Channel channel, Throwable exception) {
                turnOffReandess(exception);
            }

            @Override
            public void handleBlockedListenerException(Connection connection, Throwable exception) {
                turnOffReandess(exception);
            }

            @Override
            public void handleConsumerException(Channel channel, Throwable exception, Consumer consumer, String consumerTag, String methodName) {
                turnOffReandess(exception);
            }

            @Override
            public void handleConnectionRecoveryException(Connection conn, Throwable exception) {
                turnOffReandess(exception);
            }

            @Override
            public void handleChannelRecoveryException(Channel ch, Throwable exception) {
                turnOffReandess(exception);
            }

            @Override
            public void handleTopologyRecoveryException(Connection conn, Channel ch, TopologyRecoveryException exception) {
                turnOffReandess(exception);
            }

            private void turnOffReandess(Throwable exception) {
                CommonMetrics.setRabbitMQReadiness(false);
                LOGGER.debug("Set RabbitMQ readiness to false. RabbitMQ error", exception);
            }
        });

        factory.setAutomaticRecoveryEnabled(true);
        factory.setConnectionRecoveryTriggeringCondition(shutdownSignal -> {
            if (connectionIsClosed.get()) {
                return false;
            }

            int tmpCountTriesToRecovery = connectionRecoveryAttempts.get();

            if (tmpCountTriesToRecovery < rabbitMQConfiguration.getMaxRecoveryAttempts()) {
                LOGGER.info("Try to recovery connection to RabbitMQ. Count tries = {}", tmpCountTriesToRecovery + 1);
                return true;
            }
            LOGGER.error("Can not connect to RabbitMQ. Count tries = {}", tmpCountTriesToRecovery);
            if (onFailedRecoveryConnection != null) {
                onFailedRecoveryConnection.run();
            } else {
                // TODO: we should stop the execution of the application. Don't use System.exit!!!
                throw new IllegalStateException("Cannot recover connection to RabbitMQ");
            }
            return false;
        });

        factory.setRecoveryDelayHandler(recoveryAttempts -> {
                    int tmpCountTriesToRecovery = connectionRecoveryAttempts.getAndIncrement();

                    int recoveryDelay = rabbitMQConfiguration.getMinConnectionRecoveryTimeout()
                            + (rabbitMQConfiguration.getMaxRecoveryAttempts() > 1
                                ? (rabbitMQConfiguration.getMaxConnectionRecoveryTimeout() - rabbitMQConfiguration.getMinConnectionRecoveryTimeout())
                                    / (rabbitMQConfiguration.getMaxRecoveryAttempts() - 1)
                                    * tmpCountTriesToRecovery
                                : 0);

                    LOGGER.info("Recovery delay for '{}' try = {}", tmpCountTriesToRecovery, recoveryDelay);
                    return recoveryDelay;
                }
        );

        try {
            this.connection = factory.newConnection();
            CommonMetrics.setRabbitMQReadiness(true);
            LOGGER.debug("Set RabbitMQ readiness to true");
        } catch (IOException | TimeoutException e) {
            CommonMetrics.setRabbitMQReadiness(false);
            LOGGER.debug("Set RabbitMQ readiness to false. Can not create connection", e);
            throw new IllegalStateException("Failed to create RabbitMQ connection using following configuration: " + rabbitMQConfiguration, e);
        }

        if (this.connection instanceof Recoverable) {
            Recoverable recoverableConnection = (Recoverable)this.connection;
            recoverableConnection.addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecovery(Recoverable recoverable) {
                    LOGGER.debug("Count tries to recovery connection reset to 0");
                    connectionRecoveryAttempts.set(0);
                    CommonMetrics.setRabbitMQReadiness(true);
                    LOGGER.debug("Set RabbitMQ readiness to true");
                }

                @Override
                public void handleRecoveryStarted(Recoverable recoverable) {
                }
            });
            LOGGER.debug("Recovery listener was added to connection.");
        } else {
            throw new IllegalStateException("Connection does not implement Recoverable. Can not add RecoveryListener to it");
        }

        resendRejectService = Executors.newFixedThreadPool(configuration.getResendMessageConfiguration().getResendWorkers(), new DefaultThreadFactory("th2-resend-thread"));
    }

    public boolean isOpen() {
        return connection.isOpen() && !connectionIsClosed.get();
    }

    public ResendMessageConfiguration getResendConfiguration() {
        return resendConfiguration;
    }

    @Override
    public void close() throws IllegalStateException {
        if (connectionIsClosed.getAndSet(true)) {
            return;
        }

        if (connection.isOpen()) {
            try {
                connection.close(configuration.getConnectionCloseTimeout());
            } catch (IOException e) {
                throw new IllegalStateException("Can not close connection");
            }
        }
    }

    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body, Supplier<Long> delayHandler, Runnable ackHandler) {
        try {
            basicPublish(new PublishData(exchange, routingKey, props, body, delayHandler, ackHandler));
        } catch (IOException e) {
            LOGGER.error("Can not send message with exchange '{}' and routing key '{}'", exchange, routingKey);
        }
    }

    private void basicPublish(PublishData data) throws IOException {
        Channel channel = this.channel.get();
        waitForConnectionRecovery(channel);

        long nextSequence = channel.getNextPublishSeqNo();

        rejectHandlers.get().put(nextSequence, data);
        channel.basicPublish(data.exchange, data.routingKey, data.props, data.body);
    }

    public SubscriberMonitor basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
        Channel channel = this.channel.get();
        waitForConnectionRecovery(channel);
        String tag = channel.basicConsume(queue, false, subscriberName + "_" + nextSubscriberId.getAndIncrement(), (tagTmp, delivery) -> {
            try {
                try {
                    deliverCallback.handle(tagTmp, delivery);
                } finally {
                    basicAck(channel, delivery.getEnvelope().getDeliveryTag());
                }
            } catch (IOException | RuntimeException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }, cancelCallback);

        return new RabbitMqSubscriberMonitor(channel, tag);
    }

    public void basicCancel(Channel channel, String consumerTag) throws IOException {
        waitForConnectionRecovery(channel);
        channel.basicCancel(consumerTag);
    }

    private Channel createChannel() {
        waitForConnectionRecovery(connection);

        try {
            Channel channel = connection.createChannel();

            channel.basicQos(configuration.getPrefetchCount());
            channel.confirmSelect();


            Map<Long, PublishData> currentRejectHandlers = rejectHandlers.get();

            channel.addConfirmListener(new ConfirmListener() {

                private long minSeq = -1l;

                @Override
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    if (multiple) {
                        if (minSeq < 0) {
                            minSeq = deliveryTag;
                        }

                        for (; minSeq <= deliveryTag; minSeq++) {
                            PublishData data = currentRejectHandlers.remove(minSeq);
                            if (data != null) {
                                try {
                                    data.success();
                                } catch (Exception e) {
                                    LOGGER.error("Can not execute success handler for exchange '{}', routing key '{}'", data.exchange, data.routingKey, e);
                                }
                            }
                        }

                    } else {
                        if (minSeq < 0 || minSeq + 1 == deliveryTag) {
                            minSeq = deliveryTag;
                        }

                        PublishData data = currentRejectHandlers.remove(deliveryTag);
                        if (data != null) {
                            try {
                                data.success();
                            } catch (Exception e) {
                                LOGGER.error("Can not execute success handler for exchange '{}', routing key '{}'", data.exchange, data.routingKey, e);
                            }
                        }
                    }
                }

                @SneakyThrows
                @Override
                public void handleNack(long deliveryTag, boolean multiple) throws IOException {

                    if (multiple) {
                        if (minSeq < 0) {
                            minSeq = deliveryTag;
                        }

                        for (; minSeq <= deliveryTag; minSeq++) {

                            LOGGER.trace("Message with delivery tag '{}' is rejected", minSeq);

                            PublishData data = currentRejectHandlers.remove(minSeq);

                            if (data != null) {
                                resendRejectService.submit(data::resend);
                            }
                        }
                    } else {
                        if (minSeq < 0 || minSeq + 1 == deliveryTag) {
                            minSeq = deliveryTag;
                        }

                        LOGGER.trace("Message with delivery tag '{}' is rejected", deliveryTag);

                        PublishData data = currentRejectHandlers.remove(deliveryTag);

                        if (data != null) {
                            resendRejectService.submit(data::resend);
                        }
                    }
                }
            });
            return channel;
        } catch (IOException e) {
            throw new IllegalStateException("Can not create channel", e);
        }
    }

    private void waitForConnectionRecovery(ShutdownNotifier notifier) {
        while (!notifier.isOpen() && !connectionIsClosed.get()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                LOGGER.error("Wait for connection recovery was interrupted", e);
                break;
            }
        }

        if (connectionIsClosed.get()) {
            throw new IllegalStateException("Connection is already closed");
        }
    }

    /**
     * @param channel pass channel witch used for basicConsume, because delivery tags are scoped per channel,
     *                deliveries must be acknowledged on the same channel they were received on.
     * @throws IOException
     */
    private void basicAck(Channel channel, long deliveryTag) throws IOException {
        waitForConnectionRecovery(channel);
        channel.basicAck(deliveryTag, false);
    }

    private class RabbitMqSubscriberMonitor implements SubscriberMonitor {

        private final Channel channel;
        private final String tag;

        public RabbitMqSubscriberMonitor(Channel channel, String tag) {
            this.channel = channel;
            this.tag = tag;
        }

        @Override
        public void unsubscribe() throws Exception {
            basicCancel(channel, tag);
        }
    }

    @Getter
    private class PublishData {

        private final String exchange;
        private final String routingKey;
        private final BasicProperties props;
        private final byte[] body;
        private final Supplier<Long> delaySupplier;
        private final Runnable ackHandler;

        public PublishData(String exchange, String routingKey, BasicProperties props, byte[] body, Supplier<Long> delaySupplier, Runnable ackHandler) {
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.props = props;
            this.body = body;
            this.delaySupplier = delaySupplier;
            this.ackHandler = ackHandler;
        }

        public void send() {
            try {
                basicPublish(this);
            } catch (IOException e) {
                LOGGER.error("Can not send message to exchange '{}', routing key '{}'", exchange, routingKey);
            }
        }

        public void success() {
            if (ackHandler != null) {
                try {
                    ackHandler.run();
                } catch (Exception e) {
                    LOGGER.warn("Can not execute ack handler for exchange '{}', routing key '{}'", exchange, routingKey, e);
                }
            }
        }

        public void resend() {
            if (delaySupplier != null) {
                long delay = 0;
                try {
                    delay = delaySupplier.get();
                } catch (Exception e) {
                    LOGGER.error("Can not get delay for message to exchange '{}', routing key '{}'", exchange, routingKey, e);
                }

                if (delay > -1) {
                    if (delay > 0) {
                        LOGGER.trace("Wait for resend message to exchange '{}', routing key '{}' milliseconds = {}", exchange, routingKey, delay);
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            LOGGER.warn("Interrupted resend message to exchange '{}', routing key '{}'", exchange, routingKey, e);
                        }
                    }

                    send();
                }
            }
        }
    }
}

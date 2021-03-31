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
package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection;

import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ResendMessageConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.retry.RetryBuilder;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.retry.RetryRequest;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Recoverable;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);
    private static final long DEFAULT_CLOSE_TIMEOUT = 10_000;

    private final HealthMetrics managerMetrics = new HealthMetrics(
            CommonMetrics.registerLiveness(this),
            CommonMetrics.registerReadiness(this)
    );

    private final Connection connection;
    private final AtomicBoolean connectionIsClosed = new AtomicBoolean(false);
    private final AtomicInteger nextSubscriberId = new AtomicInteger(1);
    private final RabbitMQConfiguration configuration;
    private final String subscriberName;

    private final ScheduledExecutorService scheduler;
    private final ForkJoinPool tasker;

    public ConnectionManager(@NotNull RabbitMQConfiguration rabbitMQConfiguration, @NotNull ScheduledExecutorService scheduledExecutorService, Runnable onFailedRecoveryConnection) {
        this.configuration = Objects.requireNonNull(rabbitMQConfiguration, "RabbitMQ configuration cannot be null");
        ResendMessageConfiguration resendConfiguration = Objects.requireNonNull(configuration.getResendMessageConfiguration(), "Resend configuration can not be null");

        ConnectionRecoveryManager recoveryManager = new ConnectionRecoveryManager(rabbitMQConfiguration, onFailedRecoveryConnection, connectionIsClosed);
        if (StringUtils.isBlank(rabbitMQConfiguration.getSubscriberName())) {
            subscriberName = "rabbit_mq_subscriber." + System.currentTimeMillis();
            LOGGER.info("Subscribers will use default name: {}", subscriberName);
        } else {
            subscriberName = rabbitMQConfiguration.getSubscriberName() + "." + System.currentTimeMillis();
        }

        var factory = new ConnectionFactory();
        var virtualHost = rabbitMQConfiguration.getVhost();
        var username = rabbitMQConfiguration.getUsername();
        var password = rabbitMQConfiguration.getPassword();

        this.scheduler = Objects.requireNonNull(scheduledExecutorService, "Scheduler can not be null");
        this.tasker = new ForkJoinPool(resendConfiguration.getMaxResendWorkers());

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

        factory.setExceptionHandler(recoveryManager);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setConnectionRecoveryTriggeringCondition(recoveryManager);
        factory.setRecoveryDelayHandler(recoveryManager);

        try {
            this.connection = factory.newConnection();
            LOGGER.debug("Set RabbitMQ readiness to true");
        } catch (IOException | TimeoutException e) {
            managerMetrics.disable();
            LOGGER.debug("Set RabbitMQ readiness to false. Can not create connection", e);
            throw new IllegalStateException("Failed to create RabbitMQ connection using following configuration: " + rabbitMQConfiguration, e);
        }

        this.connection.addBlockedListener(new BlockedListener() {
            @Override
            public void handleBlocked(String reason) throws IOException {
                LOGGER.warn("RabbitMQ blocked connection: {}", reason);
            }

            @Override
            public void handleUnblocked() throws IOException {
                LOGGER.warn("RabbitMQ unblocked connection");
            }
        });

        if (this.connection instanceof Recoverable) {
            Recoverable recoverableConnection = (Recoverable) this.connection;
            recoverableConnection.addRecoveryListener(recoveryManager);
            LOGGER.debug("Recovery listener was added to connection.");
        } else {
            LOGGER.warn("Connection does not implement Recoverable. Can not add RecoveryListener to it");
        }
    }

    public <T> RetryBuilder<T> createRetryBuilder() {
        return new RetryBuilder<>(configuration, scheduler, connectionIsClosed, this::createChannel);
    }

    public RabbitMQConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Using common metrics.
     *
     * @see ConnectionManager#basicConsume(String, DeliverCallback, CancelCallback, HealthMetrics)
     */
    @Deprecated(forRemoval = true)
    public SubscriberMonitor basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        return basicConsume(queue, deliverCallback, cancelCallback, managerMetrics);
    }

    public SubscriberMonitor basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback, HealthMetrics metrics) {
        String consumerTag = subscriberName + '_' + nextSubscriberId.getAndIncrement();
        CompletableFuture<String> future = this.<String>createRetryBuilder()
//                .setFunction()
                .setMetrics(metrics)
                .build(channel -> channel.basicConsume(queue, false, consumerTag, (tag, delivery) -> {
                        try {
                            deliverCallback.handle(tag, delivery);
                        } catch (IOException e) {
                            LOGGER.error("Can not handle delivery for consumer with tag '{}'", consumerTag, e);
                        } finally {
                            basicAck(channel, delivery.getEnvelope().getDeliveryTag(), metrics);
                        }
                    }, cancelCallback));
        return new RabbitMqSubscriberMonitor(future, subscriberName);
    }

    /**
     * Using common metrics.
     *
     * @see ConnectionManager#basicCancel(String, HealthMetrics)
     */
    @Deprecated(forRemoval = true)
    public void basicCancel(Channel channel, String consumerTag) {
        createRetryBuilder()
//                .setChannelCreator(() -> channel)
                .setMetrics(managerMetrics)
                .build(channel1 -> {
                    channel1.basicCancel(consumerTag);
                    return null;
                })
                .exceptionally(ex -> {
                    LOGGER.error("Can not cancel consumer with tag '{}'", consumerTag, ex);
                    return null;
                });
    }

    public void basicCancel(String consumerTag, HealthMetrics metrics) {
        createRetryBuilder()
                .setMetrics(metrics)
                .build(channel1 -> {
                    channel1.basicCancel(consumerTag);
                    return null;
                })
                .exceptionally(ex -> {
                    LOGGER.error("Can not cancel consumer with tag '{}'", consumerTag, ex);
                    return null;
                });
    }

    @Override
    public void close() throws IllegalStateException {
        if (connectionIsClosed.getAndSet(true)) {
            return;
        }


        try {
            tasker.shutdown();
            if (!tasker.awaitTermination(DEFAULT_CLOSE_TIMEOUT, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Thread pool cannot be shutdown for {} milliseconds", DEFAULT_CLOSE_TIMEOUT);
                tasker.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Can not close thread pool in ConnectionManager", e);
        }

        try {
            connection.close(configuration.getConnectionCloseTimeout());
        } catch (IOException e) {
            LOGGER.warn("Can not close RabbitMQ connection. Try to abort it");
            connection.abort(configuration.getConnectionCloseTimeout());
        }
    }

    public Channel createChannelWithoutCheck() throws IOException {
        return connection.createChannel();
    }

    public Channel createChannel() {
        checkConnection();

        RetryRequest<Channel> request = new RetryRequest<>(connectionIsClosed, configuration, scheduler, managerMetrics.getLivenessMonitor(), managerMetrics.getReadinessMonitor()) {
            @Override
            protected Channel action(Channel channel) throws IOException, AlreadyClosedException {
                return connection.createChannel();
            }
        };

        CompletableFuture<Channel> future = request.getCompletableFuture();
        tasker.execute(request);

        Channel channel;
        try {
            channel = future.get();
        } catch (InterruptedException | ExecutionException | CancellationException e) {
            managerMetrics.disable();
            throw new IllegalStateException("Can not create channel from RabbitMQ connection", e);
        }

        try {
            channel.confirmSelect();
        } catch (IOException e) {
            LOGGER.warn("Can not set confirm select to channel", e);
        }

        channel.addReturnListener(ret ->
                LOGGER.warn("Can not router message to exchange '{}', routing key '{}'. Reply code '{}' and text = {}", ret.getExchange(), ret.getRoutingKey(), ret.getReplyCode(), ret.getReplyText()));

        try {
            channel.basicQos(configuration.getPrefetchCount());
        } catch (IOException e) {
            LOGGER.warn("Can not set prefetch number. Wrong number for prefetch. It must be from 0 to 65535", e);
        }

        return channel;
    }

    private void checkConnection() {
        if (connectionIsClosed.get()) {
            throw new IllegalStateException("Connection is already closed");
        }
    }

    /**
     * @param channel pass channel witch used for basicConsume, because delivery tags are scoped per channel,
     *                deliveries must be acknowledged on the same channel they were received on.
     */
    private void basicAck(Channel channel, long deliveryTag, HealthMetrics metrics) {
        try {
            channel.basicAck(deliveryTag, true);
        } catch (IOException | AlreadyClosedException e) {
            metrics.getLivenessMonitor().disable();
            metrics.getReadinessMonitor().disable();
            LOGGER.error("Can not send basic ack for delivery tag = " + deliveryTag);
        }
    }

    private class RabbitMqSubscriberMonitor implements SubscriberMonitor {
        private static final String PREFIX_METRIC_NAME = "subscriber_monitor_consumer_";
        private final CompletableFuture<String> futureTag;
        private final HealthMetrics healthMetrics;

        public RabbitMqSubscriberMonitor(CompletableFuture<String> futureTag, String consumerTag) {
            this.futureTag = futureTag;

            healthMetrics = new HealthMetrics(
                    CommonMetrics.registerLiveness(PREFIX_METRIC_NAME + consumerTag + "_liveness"),
                    CommonMetrics.registerReadiness(PREFIX_METRIC_NAME + consumerTag + "_readiness")
            );
        }

        @Override
        public void unsubscribe() {
            try {
                basicCancel(futureTag.get(), healthMetrics);
            } catch (InterruptedException | ExecutionException | CancellationException e) {
                LOGGER.warn("Can not unsubscribe from queue", e);
            }
        }
    }
}

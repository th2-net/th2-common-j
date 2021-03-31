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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private final HealthMetrics managerMetrics = new HealthMetrics(
            CommonMetrics.registerLiveness(this),
            CommonMetrics.registerReadiness(this)
    );

    private final Connection connection;
    private final AtomicBoolean connectionIsClosed = new AtomicBoolean(false);
    private final AtomicInteger nextSubscriberId = new AtomicInteger(1);
    private final RabbitMQConfiguration configuration;
    private final String subscriberName;

    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder()
            .setNameFormat("th2-common-retry-%d")
            .build());

    private Channel channel = null;

    public ConnectionManager(@NotNull RabbitMQConfiguration rabbitMQConfiguration, Runnable onFailedRecoveryConnection) {
        this.configuration = Objects.requireNonNull(rabbitMQConfiguration, "RabbitMQ configuration cannot be null");

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

    public RabbitMQConfiguration getConfiguration() {
        return configuration;
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    public Channel createChannelWithoutCheck() throws IOException {
        return connection.createChannel();
    }

    public SubscriberMonitor basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback, HealthMetrics metrics) {
        CompletableFuture<String> future = new CompletableFuture<>();
        callOnChannelAsync(future, channel -> {
            String consumerTag = subscriberName + '_' + nextSubscriberId.getAndIncrement();

            future.complete(channel.basicConsume(queue, false, consumerTag, (tag, delivery) -> {
                try {
                    deliverCallback.handle(tag, delivery);
                } catch (IOException e) {
                    LOGGER.error("Can not handle delivery for consumer with tag '{}'", consumerTag, e);
                } finally {
                    basicAck(delivery.getEnvelope().getDeliveryTag(), metrics);
                }
            }, cancelCallback));
        }, 1, metrics);

        try {
            return new RabbitMqSubscriberMonitor(future.get());
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Can not subscribe to queue = {}", queue);
            metrics.disable();
        }
        return null;
    }

    public void basicCancel(String consumerTag, HealthMetrics metrics) {
        try {
            callOnChannelAsync(channel -> {
                channel.basicCancel(consumerTag);
            }, metrics);
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Can not cancel consumer with tag = {}", consumerTag);
        }
    }

    @Override
    public void close() throws IllegalStateException {
        if (connectionIsClosed.getAndSet(true)) {
            return;
        }

        try {
            if(!scheduler.isTerminated()) {
                scheduler.shutdown();
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    LOGGER.warn("Connection manager scheduler didn't shut down gracefully");
                    scheduler.shutdownNow();
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("Can not close scheduler in ConnectionManager", e);
        }

        try {
            connection.close(configuration.getConnectionCloseTimeout());
        } catch (IOException e) {
            LOGGER.warn("Can not close RabbitMQ connection. Try to abort it");
            connection.abort(configuration.getConnectionCloseTimeout());
        }
    }

    private void prepareChannel(Channel channel) {
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
    }

    private long getNextDelay(int attempts) {
        long maxDelay = configuration.getResendMessageConfiguration().getMaxDelay();
        long minDelay = configuration.getResendMessageConfiguration().getMinDelay();

        return Math.min((maxDelay - minDelay) * attempts, maxDelay);
    }

    private void checkConnection() {
        if (connectionIsClosed.get()) {
            throw new IllegalStateException("Connection is already closed");
        }
    }

    private void basicAck(long deliveryTag, HealthMetrics metrics) {
        checkConnection();

        // If we get error delivery tag is wrong or channel is close. If channel is close delivery tag is invalidated
        scheduler.schedule(() -> {
            try {
                callOnChannelSync(channel -> {
                    channel.basicAck(deliveryTag, true);
                }, metrics);
            } catch (Exception e) {
                LOGGER.error("Can not execute basic ack for deliveryTag '{}'", deliveryTag, e);
            }
        }, 0, TimeUnit.MILLISECONDS);
    }

    private void callOnChannelAsync(ChannelAction action, HealthMetrics metrics) throws ExecutionException, InterruptedException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        callOnChannelAsync(future, channel -> {
            action.apply(channel);
            future.complete(null);
        }, 1, metrics);
        future.get();
    }

    private <T> void callOnChannelAsync(CompletableFuture<T> future, ChannelAction action, int attempts, HealthMetrics metrics) {

        checkConnection();

        if (future.isCancelled()) {
            return;
        }

        long nextDelay = getNextDelay(attempts);
        LOGGER.info("Create request with delay '{}'. Current attempt = {}", nextDelay, attempts);

        scheduler.schedule(() -> {
            if (future.isCancelled()) {
                return;
            }

            try {
                callOnChannelSync(action, metrics);
            } catch (IOException | ShutdownSignalException e) {
                LOGGER.warn("Can not execute RabbitMQ request", e);
                callOnChannelAsync(future, action, attempts + 1, metrics);
            } catch (Exception e) {
                LOGGER.warn("Can not execute RabbitMQ request", e);
                future.completeExceptionally(e);
            }
        }, nextDelay, TimeUnit.MILLISECONDS);
    }

    private void callOnChannelSync(ChannelAction channelAction, HealthMetrics metrics) throws Exception {
        try {
            if (channel == null) {
                LOGGER.info("Try to create new channel");
                channel = createChannelWithoutCheck();
                prepareChannel(channel);
                LOGGER.info("Created new channel {}", channel.getChannelNumber());
            }

            channelAction.apply(channel);
            metrics.enable();
        } catch (IOException | AlreadyClosedException e) {
            metrics.notReady();
            if (channel != null) {
                try {
                    channel.abort();
                } catch (IOException ex) {
                    LOGGER.warn("Can not abort channel from retry request for RabbitMQ", ex);
                } finally {
                    channel = null;
                }
            }
            throw e;
        }
    }

    private class RabbitMqSubscriberMonitor implements SubscriberMonitor {
        private static final String PREFIX_METRIC_NAME = "subscriber_monitor_consumer_";
        private final String consumerTag;
        private final HealthMetrics healthMetrics;

        public RabbitMqSubscriberMonitor(String consumerTag) {
            this.consumerTag = consumerTag;

            healthMetrics = new HealthMetrics(
                    CommonMetrics.registerLiveness(PREFIX_METRIC_NAME + consumerTag + "_liveness"),
                    CommonMetrics.registerReadiness(PREFIX_METRIC_NAME + consumerTag + "_readiness")
            );
        }

        @Override
        public void unsubscribe() {
            basicCancel(consumerTag, healthMetrics);
        }
    }
}

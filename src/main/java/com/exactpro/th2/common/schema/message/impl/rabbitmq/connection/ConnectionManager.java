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

import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.metrics.MetricArbiter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.TopologyRecoveryException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class ConnectionManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private final MetricArbiter.MetricMonitor livenessMonitor = CommonMetrics.getLIVENESS_ARBITER().register(getClass().getSimpleName() + "_liveness_" + hashCode());
    private final MetricArbiter.MetricMonitor readinessMonitor = CommonMetrics.getREADINESS_ARBITER().register(getClass().getSimpleName() + "_readiness_" + hashCode());

    private final Connection connection;
    private final AtomicBoolean connectionIsClosed = new AtomicBoolean(false);
    private final AtomicInteger nextSubscriberId = new AtomicInteger(1);
    private final RabbitMQConfiguration configuration;
    private final ResendMessageConfiguration resendConfiguration;
    private final String subscriberName;

    private final ScheduledExecutorService executor;
    private ForkJoinPool tasker = new ForkJoinPool();

    public ConnectionManager(@NotNull RabbitMQConfiguration rabbitMQConfiguration, @NotNull ScheduledExecutorService scheduledExecutorService, Runnable onFailedRecoveryConnection) {
        this.configuration = Objects.requireNonNull(rabbitMQConfiguration, "RabbitMQ configuration cannot be null");
        this.resendConfiguration = configuration.getResendMessageConfiguration();

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

        this.executor = Objects.requireNonNull(scheduledExecutorService, "Scheduler can not be null");
        taskExecutor = new ForkJoinPool(resendConfiguration.getMaxResendWorkers());

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

        if (rabbitMQConfiguration.getConnectionTimeout() >  0) {
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
            readinessMonitor.disable();
            livenessMonitor.disable();
            LOGGER.debug("Set RabbitMQ readiness to false. Can not create connection", e);
            throw new IllegalStateException("Failed to create RabbitMQ connection using following configuration: " + rabbitMQConfiguration, e);
        }

        if (this.connection instanceof Recoverable) {
            Recoverable recoverableConnection = (Recoverable) this.connection;
            recoverableConnection.addRecoveryListener(recoveryManager);
            LOGGER.debug("Recovery listener was added to connection.");
        } else {
            LOGGER.warn("Connection does not implement Recoverable. Can not add RecoveryListener to it");
        }
    }

    /**
     * Send data only one time, without resends on reject and using common metrics
     * @see ConnectionManager#basicPublish(String, String, BasicProperties, byte[], Supplier, Runnable, MainMetrics)
     */
    @Deprecated(forRemoval = true)
    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body) {
        basicPublish(exchange, routingKey, props, body, new HealthMetrics(livenessMonitor, readinessMonitor));
    }

    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body, HealthMetrics metrics) {
        Channel channel = this.channel.get();
        basicAction(() -> {
            channel.basicPublish(exchange, routingKey, props, body);
            return null;
        }, metrics.getLivenessMonitor(), metrics.getReadinessMonitor());
    /**
     * Send data only one time, without resends on reject
     * @see ConnectionManager#basicPublish(String, String, BasicProperties, byte[], Supplier, Runnable, MainMetrics)
     */
    @Deprecated(forRemoval = true)
    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body, MainMetrics metrics) {
        basicPublish(new PublishData(exchange, routingKey, props, body, metrics));
    }

//        private void basicPublish(PublishData data) {
//            ChannelContext channelContext = this.channelContext.get();
//            Channel channel = channelContext.getChannel();
//            basicAction(() -> {
//                long nextSequence = channel.getNextPublishSeqNo();
//                channelContext.getRejectHandlers().put(nextSequence, data);
//                channel.basicPublish(data.exchange, data.routingKey, data.props, data.body);
//                return null;
//            }, data.metrics);
//        }

        /**
         * Using common metrics.
         * @see ConnectionManager#basicConsume(String, DeliverCallback, CancelCallback, MainMetrics)
         */
        @Deprecated(forRemoval = true)
    public SubscriberMonitor basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        return basicConsume(queue, deliverCallback, cancelCallback, new HealthMetrics(livenessMonitor, readinessMonitor));
    }

    public SubscriberMonitor basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback, HealthMetrics metrics) {
        Channel channel = this.createChannel();
        String consumerTag = subscriberName + '_' + nextSubscriberId.getAndIncrement();
        CompletableFuture<String> future = basicRetry(new RetryAction<>(channel, metrics.getLivenessMonitor(), metrics.getReadinessMonitor()) {
            @Override
            protected String action(Channel channel) throws IOException, AlreadyClosedException {
                return channel.basicConsume(queue, false, consumerTag, (tag, delivery) -> {
                    try {
                        try {
                            deliverCallback.handle(tag, delivery);
                        } finally {
                            try {
                                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                            } catch (IOException e) {
                                LOGGER.error("Can not create ack request", e);
                            }
                        }
                    } catch (IOException e) {
                        LOGGER.error("Can not handle delivery for consumer with tag '{}'", consumerTag, e);
                    }
                }, cancelCallback);
            }
        });

        return new RabbitMqSubscriberMonitor(channel, future, subscriberName);
    }

    /**
     * Using common metrics.
     * @see ConnectionManager#basicCancel(Channel, String, MainMetrics)
     */
    @Deprecated(forRemoval = true)
    public void basicCancel(Channel channel, String consumerTag) {
        basicCancel(channel, consumerTag, new HealthMetrics(livenessMonitor, readinessMonitor));
    }

    public void basicCancel(Channel channel, String consumerTag, HealthMetrics metrics) {
        basicRetry(new RetryAction<Void>(channel, metrics.getLivenessMonitor(), metrics.getReadinessMonitor()) {
            @Override
            protected Void action(Channel channel) throws IOException, AlreadyClosedException {
                channel.basicCancel(consumerTag);
                return null;
            }
        });
        //TODO: Add on exception or cancel callback
    }

    @Override
    public void close() throws IllegalStateException {
        if (connectionIsClosed.getAndSet(true)) {
            return;
        }

        try {
            connection.close(configuration.getConnectionCloseTimeout());
        } catch (IOException e) {
            LOGGER.warn("Can not close RabbitMQ connection. Try to abort it");
            connection.abort(configuration.getConnectionCloseTimeout());
        }
    }

    private Channel createChannel() {
        checkConnection();

        CompletableFuture<Channel> future = basicRetry(new RetryAction<Channel>(() -> null, livenessMonitor, readinessMonitor) {
            @Override
            protected Channel action(Channel channel) throws IOException, AlreadyClosedException {
                return connection.createChannel();
            }
        });

        Channel channel;
        try {
            channel = future.get();
        } catch (InterruptedException | ExecutionException | CancellationException e) {
            readinessMonitor.disable();
            livenessMonitor.disable();
            throw new IllegalStateException("Can not create channel from RabbitMQ connection", e);
        }

        //basicAction(channel::confirmSelect, metrics);

        channel.addReturnListener(ret ->
                LOGGER.warn("Can not router message to exchange '{}', routing key '{}'. Reply code '{}' and text = {}", ret.getExchange(), ret.getRoutingKey(), ret.getReplyCode(), ret.getReplyText()));

        try {
            channel.basicQos(configuration.getPrefetchCount());
        } catch (IOException e) {
            LOGGER.warn("Can not set prefetch number. Wrong number for prefetch. It must be from 0 to 65535", e);
        }

        return channel;
//        return new ChannelContext(channel);
    }

    private void checkConnection() {
        if (connectionIsClosed.get()) {
            throw new IllegalStateException("Connection is already closed");
        }
    }

    /**
     * @param channel pass channel witch used for basicConsume, because delivery tags are scoped per channel,
     *                deliveries must be acknowledged on the same channel they were received on.
     * @throws IOException
     */
    private void basicAck(Channel channel, long deliveryTag, HealthMetrics metrics) throws IOException {
//        try {
//            channel.basicAck(deliveryTag, true);
//        } catch (AlreadyClosedException e) {
//
//        }
//        basicAction(chnl -> chnl.basicAck(deliveryTag, true),
//                channel, metrics.getLivenessMonitor(), metrics.getReadinessMonitor());
    }

    private class RabbitMqSubscriberMonitor implements SubscriberMonitor {

        private final Channel channel;
        private final CompletableFuture<String> futureTag;

        private final MetricArbiter.MetricMonitor livenessMonitor;
        private final MetricArbiter.MetricMonitor readinessMonitor;

        public RabbitMqSubscriberMonitor(Channel channel, CompletableFuture<String> futureTag, String consumerTag) {
            this.channel = channel;
            this.futureTag = futureTag;

            livenessMonitor = CommonMetrics.getLIVENESS_ARBITER().register("subscriber_monitor_consumer_" + consumerTag + "_liveness");
            readinessMonitor = CommonMetrics.getREADINESS_ARBITER().register("subscriber_monitor_consumer_" + consumerTag + "_readiness");
        }
        @Override
        public void unsubscribe() {
            try {
                basicCancel(channel, futureTag.get(), new HealthMetrics(livenessMonitor, readinessMonitor));
            } catch (InterruptedException | ExecutionException | CancellationException e) {
                LOGGER.warn("Can not unsubscribe from queue", e);
            }
        }
    }

    private <T> CompletableFuture<T> basicRetry(RetryAction<T> action) {
        tasker.execute(action);
        return action.getCompletableFuture();
    }

    private <T> CompletableFuture<T> basicAction(RabbitMQFunction<T> func, Channel channel, MetricArbiter.MetricMonitor livenessMonitor, MetricArbiter.MetricMonitor readinessMonitor) {
        return basicRetry(new RetryAction<T>(channel, livenessMonitor, readinessMonitor) {
            @Override
            protected T action(Channel channel) throws IOException, AlreadyClosedException {
                return func.apply(channel);
            }
        });
    }

    private <T> CompletableFuture<T> basicAction(RabbitMQFunction<T> func, Supplier<Channel> channelCreator, MetricArbiter.MetricMonitor livenessMonitor, MetricArbiter.MetricMonitor readinessMonitor) {
        return basicRetry(new RetryAction<T>(channelCreator, livenessMonitor, readinessMonitor) {
            @Override
            protected T action(Channel channel) throws IOException, AlreadyClosedException {
                return func.apply(channel);
            }
        });
    }

    private static interface RabbitMQAction {
        public void action() throws IOException, AlreadyClosedException;
    }

    private static interface RabbitMQFunction<T> {
        T apply(Channel channel) throws IOException, AlreadyClosedException;
    }

    private abstract class RetryAction<T> implements Runnable {
        private final CompletableFuture<T> completableFuture = new CompletableFuture<>();
        private final Supplier<Channel> channelCreator;
        private final MetricArbiter.MetricMonitor livenessMonitor;
        private final MetricArbiter.MetricMonitor readinessMonitor;

        private int attemptCount = 0;
        private Channel prevChannel = null;
        private Channel channel = null;

        public RetryAction(@NotNull Supplier<Channel> channelCreator, MetricArbiter.MetricMonitor livenessMonitor, MetricArbiter.MetricMonitor readinessMonitor) {
            this.channelCreator = Objects.requireNonNull(channelCreator, "Channel creator can not be null");
            this.livenessMonitor = livenessMonitor;
            this.readinessMonitor = readinessMonitor;
        }

        public RetryAction(Channel channel, MetricArbiter.MetricMonitor livenessMonitor, MetricArbiter.MetricMonitor readinessMonitor) {
            this(() -> channel, livenessMonitor, readinessMonitor);
        }

        @Override
        public void run() {
            if (connectionIsClosed.get()) {
                completableFuture.cancel(true);
                return;
            }

            if (channel == null) {
                Channel newChannel = channelCreator.get();
                if (newChannel == prevChannel) {
                    completableFuture.cancel(true);
                    return;
                }
                channel = newChannel;
            }

            try {
                try {
                    T result = action(channel);
                    completableFuture.complete(result);
                    return;
                } finally {
                    readinessMonitor.enable();
                    livenessMonitor.enable();
                }
            } catch (IOException e) {
                readinessMonitor.disable();
                LOGGER.warn("Can not execute basic action for RabbitMQ", e);

                if (attemptCount >= configuration.getMaxRecoveryAttempts()) {
                    livenessMonitor.disable();
                }
            } catch (AlreadyClosedException e) {
                LOGGER.warn("Channel is closed from basic action for RabbitMQ", e);

                try {
                    channel.abort();
                } catch (IOException ex) {
                    LOGGER.warn("Can not abort channel from basic action for RabbitMQ", ex);
                }

                prevChannel = channel;
                channel = null;
            }

            if (completableFuture.isCancelled()) {
                executor.schedule(this, getNextDelay(), TimeUnit.MILLISECONDS);
            }
        }

        public CompletableFuture<T> getCompletableFuture() {
            return completableFuture;
        }

        protected abstract T action(Channel channel) throws IOException, AlreadyClosedException;

        private int getNextDelay() {
            if (attemptCount < configuration.getMaxRecoveryAttempts()) {
                attemptCount++;
            }
            return 1000 / configuration.getMaxRecoveryAttempts() * attemptCount;
        }
    }

//        private static class ChannelContext implements ConfirmListener {
//
//            private final Channel channel;
//            private final Map<Long, PublishData> rejectHandlers = new ConcurrentHashMap<>();
//            private long minSeq = -1L;
//
//            private ChannelContext(Channel channel) {
//                this.channel = channel;
//                this.channel.addConfirmListener(this);
//            }
//
//            public Channel getChannel() {
//                return channel;
//            }
//
//            public Map<Long, PublishData> getRejectHandlers() {
//                return rejectHandlers;
//            }
//
//            @Override
//            public void handleAck(long deliveryTag, boolean multiple) {
//                if (multiple) {
//                    if (minSeq < 0) {
//                        minSeq = deliveryTag;
//                    }
//
//                    for (; minSeq <= deliveryTag; minSeq++) {
//                        success(rejectHandlers.remove(minSeq));
//                    }
//
//                } else {
//                    if (minSeq < 0 || minSeq + 1 == deliveryTag) {
//                        minSeq = deliveryTag;
//                    }
//
//                    success(rejectHandlers.remove(deliveryTag));
//                }
//            }
//
//            @Override
//            public void handleNack(long deliveryTag, boolean multiple) {
//                if (multiple) {
//                    if (minSeq < 0) {
//                        minSeq = deliveryTag;
//                    }
//
//                    for (; minSeq <= deliveryTag; minSeq++) {
//                        LOGGER.trace("Message with delivery tag '{}' is rejected", minSeq);
//                        resend(rejectHandlers.remove(minSeq));
//                    }
//                } else {
//                    if (minSeq < 0 || minSeq + 1 == deliveryTag) {
//                        minSeq = deliveryTag;
//                    }
//
//                    LOGGER.trace("Message with delivery tag '{}' is rejected", deliveryTag);
//                    resend(rejectHandlers.remove(deliveryTag));
//                }
//            }
//
//            private void resend(PublishData data) {
//                if (data != null) {
//                    data.resend();
//                }
//            }
//
//            private void success(PublishData data) {
//                if (data != null) {
//                    data.success();
//                }
//            }
//        }
//
//        private interface RetriableSupplier<T> {
//            T retry() throws IOException;
//        }
//
//        private class PublishData {
//
//            private final String exchange;
//            private final String routingKey;
//            private final BasicProperties props;
//            private final byte[] body;
//            private final Supplier<Long> delaySupplier;
//            private final Runnable ackHandler;
//            private final MainMetrics metrics;
//
//            public PublishData(String exchange, String routingKey, BasicProperties props, byte[] body, Supplier<Long> delaySupplier, Runnable ackHandler, MainMetrics metrics) {
//                this.exchange = exchange;
//                this.routingKey = routingKey;
//                this.props = props;
//                this.body = body;
//                this.delaySupplier = delaySupplier;
//                this.ackHandler = ackHandler;
//                this.metrics = metrics;
//            }
//
//            public PublishData(String exchange, String routingKey, BasicProperties props, byte[] body, MainMetrics metrics) {
//                this(exchange, routingKey, props, body, null, null, metrics);
//            }
//
//            public PublishData(String exchange, String routingKey, BasicProperties props, byte[] body) {
//                this(exchange, routingKey, props, body, new MainMetrics(CommonMetrics.getLIVENESS_MONITOR(), CommonMetrics.getREADINESS_MONITOR()));
//            }
//
//            public void send() {
//                basicPublish(this);
//            }
//
//            public void success() {
//                if (ackHandler != null) {
//                    try {
//                        ackHandler.run();
//                    } catch (Exception e) {
//                        LOGGER.warn("Can not execute ack handler for exchange '{}', routing key '{}'", exchange, routingKey, e);
//                    }
//                }
//            }
//
//            public void resend() {
//                if (delaySupplier != null) {
//                    long delay = 0;
//                    try {
//                        delay = delaySupplier.get();
//                    } catch (Exception e) {
//                        LOGGER.warn("Can not get delay for message to exchange '{}', routing key '{}'", exchange, routingKey, e);
//                    }
//
//                    if (delay > -1) {
//                        if (delay > 0) {
//                            LOGGER.trace("Wait for resend message to exchange '{}', routing key '{}' milliseconds = {}", exchange, routingKey, delay);
//                            executor.schedule(() -> taskExecutor.execute(this::send), delay, TimeUnit.MILLISECONDS);
//                        } else {
//                            taskExecutor.execute(this::send);
//                        }
//                    }
//                }
//            }
//        }
//
//        private static class ConnectionExceptionHandler implements ExceptionHandler {
//            @Override
//            public void handleUnexpectedConnectionDriverException(Connection conn, Throwable exception) {
//                turnOffReadness(exception);
//            }
//
//            @Override
//            public void handleReturnListenerException(Channel channel, Throwable exception) {
//                turnOffReadness(exception);
//            }
//
//            @Override
//            public void handleConfirmListenerException(Channel channel, Throwable exception) {
//                turnOffReadness(exception);
//            }
//
//            @Override
//            public void handleBlockedListenerException(Connection connection, Throwable exception) {
//                turnOffReadness(exception);
//            }
//
//            @Override
//            public void handleConsumerException(Channel channel, Throwable exception, Consumer consumer, String consumerTag, String methodName) {
//                turnOffReadness(exception);
//            }
//
//            @Override
//            public void handleConnectionRecoveryException(Connection conn, Throwable exception) {
//                turnOffReadness(exception);
//            }
//
//            @Override
//            public void handleChannelRecoveryException(Channel ch, Throwable exception) {
//                turnOffReadness(exception);
//            }
//
//            @Override
//            public void handleTopologyRecoveryException(Connection conn, Channel ch, TopologyRecoveryException exception) {
//                turnOffReadness(exception);
//            }
//
//            private void turnOffReadness(Throwable exception) {
//                CommonMetrics.setRabbitMQReadiness(false);
//                LOGGER.debug("Set RabbitMQ readiness to false. RabbitMQ error", exception);
//            }
//        }
}

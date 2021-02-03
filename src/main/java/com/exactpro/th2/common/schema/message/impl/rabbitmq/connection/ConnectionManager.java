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
import com.exactpro.th2.common.metrics.MainMetrics;
import com.exactpro.th2.common.metrics.MetricArbiter;
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
import com.rabbitmq.client.TopologyRecoveryException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class ConnectionManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private final Connection connection;
    private final ThreadLocal<ChannelContext> channelContext = ThreadLocal.withInitial(this::createChannelContext);
    private final AtomicInteger connectionRecoveryAttempts = new AtomicInteger(0);
    private final AtomicBoolean connectionIsClosed = new AtomicBoolean(false);
    private final RabbitMQConfiguration configuration;
    private final ResendMessageConfiguration resendConfiguration;
    private final String subscriberName;
    private final AtomicInteger nextSubscriberId = new AtomicInteger(1);

    private final ScheduledExecutorService executor;
    private final ForkJoinPool taskExecutor;

    private final RecoveryListener recoveryListener = new RecoveryListener() {
        @Override
        public void handleRecovery(Recoverable recoverable) {
            LOGGER.debug("Count tries to recovery connection reset to 0");
            connectionRecoveryAttempts.set(0);
            CommonMetrics.setRabbitMQReadiness(true);
            LOGGER.debug("Set RabbitMQ readiness to true");
        }

        @Override
        public void handleRecoveryStarted(Recoverable recoverable) {}
    };


    public ConnectionManager(@NotNull RabbitMQConfiguration rabbitMQConfiguration, Runnable onFailedRecoveryConnection, ScheduledExecutorService scheduledExecutorService) {
        this.configuration = Objects.requireNonNull(rabbitMQConfiguration, "RabbitMQ configuration cannot be null");
        this.resendConfiguration = configuration.getResendMessageConfiguration();

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

        executor = scheduledExecutorService;
        taskExecutor = new ForkJoinPool(resendConfiguration.getMaxResendWorkers());


//                new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
//                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
//                null,
//                false,
//                0,
//                resendConfiguration.getMaxResendWorkers(),
//                1,
//                null,
//                resendConfiguration.getResendWorkersKeepAlive(),
//                TimeUnit.MILLISECONDS);

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

        factory.setExceptionHandler(new ConnectionExceptionHandler());
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
        } catch (IOException | TimeoutException e) {
            CommonMetrics.setRabbitMQReadiness(false);
            LOGGER.debug("Can not create connection", e);
            throw new IllegalStateException("Failed to create RabbitMQ connection using following configuration: " + rabbitMQConfiguration, e);
        }

        if (this.connection instanceof Recoverable) {
            Recoverable recoverableConnection = (Recoverable) this.connection;
            recoverableConnection.addRecoveryListener(recoveryListener);
            LOGGER.debug("Recovery listener was added to connection.");
        } else {
            throw new IllegalStateException("Connection does not implement Recoverable. Can not add RecoveryListener to it");
        }
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

        if (!taskExecutor.isShutdown()) {
            taskExecutor.shutdownNow();
        }
    }

    public RabbitMQConfiguration getConfiguration() {
        return configuration;
    }

    public int getMaxConnectionRecoveryTimeout() { return configuration.getMaxConnectionRecoveryTimeout(); }

    public int getMinConnectionRecoveryTimeout() { return configuration.getMinConnectionRecoveryTimeout(); }

    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body, Supplier<Long> delayHandler, Runnable ackHandler, MainMetrics metrics) {
        basicPublish(new PublishData(exchange, routingKey, props, body, delayHandler, ackHandler, metrics));
    }

    /**
     * Send data only one time, without resends on reject and using common metrics
     * @see ConnectionManager#basicPublish(String, String, BasicProperties, byte[], Supplier, Runnable, MainMetrics)
     */
    @Deprecated(forRemoval = true)
    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body) {
        basicPublish(new PublishData(exchange, routingKey, props, body));
    }

    /**
     * Send data only one time, without resends on reject
     * @see ConnectionManager#basicPublish(String, String, BasicProperties, byte[], Supplier, Runnable, MainMetrics)
     */
    @Deprecated(forRemoval = true)
    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body, MainMetrics metrics) {
        basicPublish(new PublishData(exchange, routingKey, props, body, metrics));
    }

    /**
     * Using common metrics.
     * @see ConnectionManager#basicConsume(String, DeliverCallback, CancelCallback, MainMetrics)
     */
    @Deprecated(forRemoval = true)
    public SubscriberMonitor basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        return basicConsume(queue, deliverCallback, cancelCallback, new MainMetrics(CommonMetrics.getLIVENESS_MONITOR(), CommonMetrics.getREADINESS_MONITOR()));
    }

    public SubscriberMonitor basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback, MainMetrics metrics) {
        Channel channel = this.channelContext.get().getChannel();

        String tag = basicAction(() -> channel.basicConsume(queue, false, subscriberName + "_" + nextSubscriberId.getAndIncrement(), (tagTmp, delivery) -> {
            try {
                try {
                    deliverCallback.handle(tagTmp, delivery);
                } finally {
                    basicAck(channel, delivery.getEnvelope().getDeliveryTag(), metrics);
                }
            } catch (IOException | RuntimeException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }, cancelCallback), metrics);

        return new RabbitMqSubscriberMonitor(channel, tag);
    }

    /**
     * Using common metrics.
     * @see ConnectionManager#basicCancel(Channel, String, MainMetrics)
     */
    @Deprecated(forRemoval = true)
    public void basicCancel(Channel channel, String consumerTag) {
        basicCancel(channel, consumerTag, new MainMetrics(CommonMetrics.getLIVENESS_MONITOR(), CommonMetrics.getREADINESS_MONITOR()));
    }

    public void basicCancel(Channel channel, String consumerTag, MainMetrics metrics) {
        basicAction(() -> {
            channel.basicCancel(consumerTag);
            return null;
        }, metrics);
    }

    public void restoreChannel() {
        try {
            Channel channel = channelContext.get().getChannel();
            channel.clearReturnListeners();
            channel.clearConfirmListeners();
            channel.abort(1, "Aborted while trying to restore channel.");
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }

        channelContext.remove();
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    private ChannelContext createChannelContext() {
        checkConnection();

        MainMetrics metrics = new MainMetrics(CommonMetrics.getLIVENESS_MONITOR(), CommonMetrics.getREADINESS_MONITOR());

        Channel channel = basicAction(connection::createChannel, metrics);

        basicAction(channel::confirmSelect, metrics);
        channel.addReturnListener(ret ->
                LOGGER.warn("Can not router message to exchange '{}', routing key '{}'. Reply code '{}' and text = {}", ret.getExchange(), ret.getRoutingKey(), ret.getReplyCode(), ret.getReplyText()));

        basicAction(() -> {
            channel.basicQos(configuration.getPrefetchCount());
            return null;
        }, metrics);
        return new ChannelContext(channel);
    }

    private <T> T basicAction(RetriableSupplier<T> supplier, MainMetrics metrics) {
        checkConnection();
        int attemptsCount = 0;
        int maxAttempts = configuration.getMaxRecoveryAttempts();

        while (true) {
            attemptsCount++;

            try {
                try {
                    return supplier.retry();
                } finally {
                    metrics.getReadinessMonitor().enable();
                    metrics.getLivenessMonitor().enable();
                }
            } catch (IOException e) {
                metrics.getReadinessMonitor().disable();
                LOGGER.error(e.getMessage(), e);

                if (attemptsCount >= maxAttempts) {
                    metrics.getLivenessMonitor().disable();

                    // Infinite loop trying to do basicAction until success. attemptsCount can become more
                    // than Integer.MAX_VALUE, so making it 0.
                    attemptsCount = 0;
                }
            }
        }
    }

    private void basicPublish(PublishData data) {
        ChannelContext channelContext = this.channelContext.get();
        Channel channel = channelContext.getChannel();
        basicAction(() -> {
            long nextSequence = channel.getNextPublishSeqNo();
            channelContext.getRejectHandlers().put(nextSequence, data);
            channel.basicPublish(data.exchange, data.routingKey, data.props, data.body);
            return null;
        }, data.metrics);
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
    private void basicAck(Channel channel, long deliveryTag, MainMetrics metrics) throws IOException {
        basicAction(() -> {
            channel.basicAck(deliveryTag, false);
            return null;
        }, metrics);
    }
    private class RabbitMqSubscriberMonitor implements SubscriberMonitor {

        private final Channel channel;

        private final String tag;
        private MetricArbiter.MetricMonitor livenessMonitor;

        private MetricArbiter.MetricMonitor readinessMonitor;

        public RabbitMqSubscriberMonitor(Channel channel, String tag) {
            this.channel = channel;
            this.tag = tag;

            livenessMonitor = CommonMetrics.getLIVENESS_ARBITER().register("channel_" + tag + "_liveness");
            readinessMonitor = CommonMetrics.getREADINESS_ARBITER().register("channel_" + tag + "_readiness");
        }
        @Override
        public void unsubscribe() {
            basicCancel(channel, tag, new MainMetrics(livenessMonitor, readinessMonitor));
        }

    }

    private static class ChannelContext implements ConfirmListener {

        private final Channel channel;
        private final Map<Long, PublishData> rejectHandlers = new ConcurrentHashMap<>();
        private long minSeq = -1L;

        private ChannelContext(Channel channel) {
            this.channel = channel;
            this.channel.addConfirmListener(this);
        }

        public Channel getChannel() {
            return channel;
        }

        public Map<Long, PublishData> getRejectHandlers() {
            return rejectHandlers;
        }

        @Override
        public void handleAck(long deliveryTag, boolean multiple) {
            if (multiple) {
                if (minSeq < 0) {
                    minSeq = deliveryTag;
                }

                for (; minSeq <= deliveryTag; minSeq++) {
                    success(rejectHandlers.remove(minSeq));
                }

            } else {
                if (minSeq < 0 || minSeq + 1 == deliveryTag) {
                    minSeq = deliveryTag;
                }

                success(rejectHandlers.remove(deliveryTag));
            }
        }

        @Override
        public void handleNack(long deliveryTag, boolean multiple) {
            if (multiple) {
                if (minSeq < 0) {
                    minSeq = deliveryTag;
                }

                for (; minSeq <= deliveryTag; minSeq++) {
                    LOGGER.trace("Message with delivery tag '{}' is rejected", minSeq);
                    resend(rejectHandlers.remove(minSeq));
                }
            } else {
                if (minSeq < 0 || minSeq + 1 == deliveryTag) {
                    minSeq = deliveryTag;
                }

                LOGGER.trace("Message with delivery tag '{}' is rejected", deliveryTag);
                resend(rejectHandlers.remove(deliveryTag));
            }
        }

        private void resend(PublishData data) {
            if (data != null) {
                data.resend();
            }
        }

        private void success(PublishData data) {
            if (data != null) {
                data.success();
            }
        }
    }

    private interface RetriableSupplier<T> {
        T retry() throws IOException;
    }

    private class PublishData {

        private final String exchange;
        private final String routingKey;
        private final BasicProperties props;
        private final byte[] body;
        private final Supplier<Long> delaySupplier;
        private final Runnable ackHandler;
        private final MainMetrics metrics;

        public PublishData(String exchange, String routingKey, BasicProperties props, byte[] body, Supplier<Long> delaySupplier, Runnable ackHandler, MainMetrics metrics) {
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.props = props;
            this.body = body;
            this.delaySupplier = delaySupplier;
            this.ackHandler = ackHandler;
            this.metrics = metrics;
        }

        public PublishData(String exchange, String routingKey, BasicProperties props, byte[] body, MainMetrics metrics) {
            this(exchange, routingKey, props, body, null, null, metrics);
        }

        public PublishData(String exchange, String routingKey, BasicProperties props, byte[] body) {
            this(exchange, routingKey, props, body, new MainMetrics(CommonMetrics.getLIVENESS_MONITOR(), CommonMetrics.getREADINESS_MONITOR()));
        }

        public void send() {
            basicPublish(this);
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
                    LOGGER.warn("Can not get delay for message to exchange '{}', routing key '{}'", exchange, routingKey, e);
                }

                if (delay > -1) {
                    if (delay > 0) {
                        LOGGER.trace("Wait for resend message to exchange '{}', routing key '{}' milliseconds = {}", exchange, routingKey, delay);
                        executor.schedule(() -> taskExecutor.execute(this::send), delay, TimeUnit.MILLISECONDS);
                    } else {
                        taskExecutor.execute(this::send);
                    }
                }
            }
        }
    }

    private static class ConnectionExceptionHandler implements ExceptionHandler {
        @Override
        public void handleUnexpectedConnectionDriverException(Connection conn, Throwable exception) {
            turnOffReadness(exception);
        }

        @Override
        public void handleReturnListenerException(Channel channel, Throwable exception) {
            turnOffReadness(exception);
        }

        @Override
        public void handleConfirmListenerException(Channel channel, Throwable exception) {
            turnOffReadness(exception);
        }

        @Override
        public void handleBlockedListenerException(Connection connection, Throwable exception) {
            turnOffReadness(exception);
        }

        @Override
        public void handleConsumerException(Channel channel, Throwable exception, Consumer consumer, String consumerTag, String methodName) {
            turnOffReadness(exception);
        }

        @Override
        public void handleConnectionRecoveryException(Connection conn, Throwable exception) {
            turnOffReadness(exception);
        }

        @Override
        public void handleChannelRecoveryException(Channel ch, Throwable exception) {
            turnOffReadness(exception);
        }

        @Override
        public void handleTopologyRecoveryException(Connection conn, Channel ch, TopologyRecoveryException exception) {
            turnOffReadness(exception);
        }

        private void turnOffReadness(Throwable exception) {
            CommonMetrics.setRabbitMQReadiness(false);
            LOGGER.debug("Set RabbitMQ readiness to false. RabbitMQ error", exception);
        }
    }
}

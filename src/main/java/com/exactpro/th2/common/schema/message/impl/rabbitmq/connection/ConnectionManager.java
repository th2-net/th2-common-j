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

import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownNotifier;
import com.rabbitmq.client.TopologyRecoveryException;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class ConnectionManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private static final String PUBLISH_CONNECTION_NAME = "Publish";
    private static final String CONSUME_CONNECTION_NAME = "Consume";

    private final Connection publishConnection;
    private final Connection consumeConnection;
    private final Map<PinId, ChannelHolder> channelsByPin = new ConcurrentHashMap<>();
    private final AtomicInteger publishConnectionRecoveryAttempts = new AtomicInteger(0);
    private final AtomicInteger consumeConnectionRecoveryAttempts = new AtomicInteger(0);
    private final AtomicBoolean connectionIsClosed = new AtomicBoolean(false);
    private final ConnectionManagerConfiguration configuration;
    private final String subscriberName;
    private final AtomicInteger nextSubscriberId = new AtomicInteger(1);
    private final ExecutorService publishSharedExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("rabbitmq-shared-pool-%d")
            .build());
    private final ExecutorService consumeSharedExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("rabbitmq-shared-pool-%d")
            .build());

    private final HealthMetrics publishMetrics = new HealthMetrics(this);
    private final HealthMetrics consumeMetrics = new HealthMetrics(this);

    private final RecoveryListener publishRecoveryListener = getRecoveryListener(PUBLISH_CONNECTION_NAME);
    private final RecoveryListener consumehRecoveryListener = getRecoveryListener(CONSUME_CONNECTION_NAME);

    public ConnectionManagerConfiguration getConfiguration() {
        return configuration;
    }

    public ConnectionManager(@NotNull RabbitMQConfiguration rabbitMQConfiguration, @NotNull ConnectionManagerConfiguration connectionManagerConfiguration, Runnable onFailedRecoveryConnection) {
        Objects.requireNonNull(rabbitMQConfiguration, "RabbitMQ configuration cannot be null");
        this.configuration = Objects.requireNonNull(connectionManagerConfiguration, "Connection manager configuration can not be null");

        String subscriberNameTmp = ObjectUtils.defaultIfNull(connectionManagerConfiguration.getSubscriberName(), rabbitMQConfiguration.getSubscriberName());
        if (StringUtils.isBlank(subscriberNameTmp)) {
            subscriberName = "rabbit_mq_subscriber." + System.currentTimeMillis();
            LOGGER.info("Subscribers will use default name: {}", subscriberName);
        } else {
            subscriberName = subscriberNameTmp + "." + System.currentTimeMillis();
        }

        var factory = new ConnectionFactory();
        var virtualHost = rabbitMQConfiguration.getVHost();
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

        if (connectionManagerConfiguration.getConnectionTimeout() > 0) {
            factory.setConnectionTimeout(connectionManagerConfiguration.getConnectionTimeout());
        }
        factory.setAutomaticRecoveryEnabled(true);
        this.publishConnection = createConnection(PUBLISH_CONNECTION_NAME, factory, connectionManagerConfiguration, onFailedRecoveryConnection);
        this.consumeConnection = createConnection(CONSUME_CONNECTION_NAME, factory, connectionManagerConfiguration, onFailedRecoveryConnection);

    }

    public boolean isOpen() {
        return (publishConnection.isOpen() || consumeConnection.isOpen()) && !connectionIsClosed.get();
    }

    @Override
    public void close() {
        if (connectionIsClosed.getAndSet(true)) {
            return;
        }

        int closeTimeout = configuration.getConnectionCloseTimeout();
        closeConnection(publishConnection, PUBLISH_CONNECTION_NAME, closeTimeout);
        closeConnection(consumeConnection, CONSUME_CONNECTION_NAME, closeTimeout);

        shutdownSharedExecutor(closeTimeout, publishSharedExecutor);
        shutdownSharedExecutor(closeTimeout, consumeSharedExecutor);
    }

    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body) throws IOException {
        ChannelHolder holder = getChannelFor(PinId.forRoutingKey(routingKey), publishConnection);
        holder.withLock(channel -> channel.basicPublish(exchange, routingKey, props, body));
    }

    public SubscriberMonitor basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
        ChannelHolder holder = getChannelFor(PinId.forQueue(queue), consumeConnection);
        String tag = holder.mapWithLock(channel -> {
            return channel.basicConsume(queue, false, subscriberName + "_" + nextSubscriberId.getAndIncrement(), (tagTmp, delivery) -> {
                    try {
                        try {
                            deliverCallback.handle(tagTmp, delivery);
                        } finally {
                            holder.withLock(ch -> basicAck(ch, delivery.getEnvelope().getDeliveryTag()));
                        }
                    } catch (IOException | RuntimeException e) {
                        LOGGER.error("Cannot handle delivery for tag {}: {}", tagTmp, e.getMessage(), e);
                    }
            }, cancelCallback);
        });

        return new RabbitMqSubscriberMonitor(holder, tag, this::basicCancel);
    }

    private void basicCancel(Channel channel, String consumerTag) throws IOException {
        channel.basicCancel(consumerTag);
    }

    private static void shutdownSharedExecutor(int closeTimeout, ExecutorService sharedExecutor) {
        sharedExecutor.shutdown();
        try {
            if (!sharedExecutor.awaitTermination(closeTimeout, TimeUnit.MILLISECONDS)) {
                LOGGER.error("Executor is not terminated during {} millis", closeTimeout);
                List<Runnable> runnables = sharedExecutor.shutdownNow();
                LOGGER.error("{} task(s) was(were) not finished", runnables.size());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ChannelHolder getChannelFor(PinId pinId, Connection currentConnection) {
        return channelsByPin.computeIfAbsent(pinId, ignore -> {
            LOGGER.trace("Creating channel holder for {}", pinId);
            return new ChannelHolder(() -> createChannel(currentConnection), this::waitForConnectionRecovery);
        });
    }

    private Channel createChannel(Connection connection) {
        waitForConnectionRecovery(connection);

        try {
            Channel channel = connection.createChannel();
            Objects.requireNonNull(channel, () -> "No channels are available in the connection. Max channel number: " + connection.getChannelMax());
            channel.basicQos(configuration.getPrefetchCount());
            channel.addReturnListener(ret ->
                    LOGGER.warn("Can not router message to exchange '{}', routing key '{}'. Reply code '{}' and text = {}", ret.getExchange(), ret.getRoutingKey(), ret.getReplyCode(), ret.getReplyText()));
            return channel;
        } catch (IOException e) {
            throw new IllegalStateException("Can not create channel", e);
        }
    }

    private void waitForConnectionRecovery(ShutdownNotifier notifier) {
        waitForConnectionRecovery(notifier, true);
    }

    private void waitForConnectionRecovery(ShutdownNotifier notifier, boolean waitForRecovery) {
        if (isConnectionRecovery(notifier)) {
            if (waitForRecovery) {
                waitForRecovery(notifier);
            } else {
                LOGGER.warn("Skip waiting for connection recovery");
            }
        }

        if (connectionIsClosed.get()) {
            throw new IllegalStateException("Connection is already closed");
        }
    }

    private void waitForRecovery(ShutdownNotifier notifier) {
        LOGGER.warn("Start waiting for connection recovery");
        while (isConnectionRecovery(notifier)) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                LOGGER.error("Wait for connection recovery was interrupted", e);
                break;
            }
        }
        LOGGER.info("Stop waiting for connection recovery");
    }

    private boolean isConnectionRecovery(ShutdownNotifier notifier) {
        return !notifier.isOpen() && !connectionIsClosed.get();
    }

    /**
     * @param channel pass channel witch used for basicConsume, because delivery tags are scoped per channel,
     *                deliveries must be acknowledged on the same channel they were received on.
     * @throws IOException
     */
    private void basicAck(Channel channel, long deliveryTag) throws IOException {
        channel.basicAck(deliveryTag, false);
    }

    private Connection createConnection(String connectionName, ConnectionFactory factory, ConnectionManagerConfiguration connectionManagerConfiguration, Runnable onFailedRecoveryConnection) {
        factory.setExceptionHandler(new ExceptionHandler() {
            @Override
            public void handleUnexpectedConnectionDriverException(Connection conn, Throwable exception) {
                turnOffReadiness(exception);
            }

            @Override
            public void handleReturnListenerException(Channel channel, Throwable exception) {
                turnOffReadiness(exception);
            }

            @Override
            public void handleConfirmListenerException(Channel channel, Throwable exception) {
                turnOffReadiness(exception);
            }

            @Override
            public void handleBlockedListenerException(Connection connection, Throwable exception) {
                turnOffReadiness(exception);
            }

            @Override
            public void handleConsumerException(Channel channel, Throwable exception, Consumer consumer, String consumerTag, String methodName) {
                turnOffReadiness(exception);
            }

            @Override
            public void handleConnectionRecoveryException(Connection conn, Throwable exception) {
                turnOffReadiness(exception);
            }

            @Override
            public void handleChannelRecoveryException(Channel ch, Throwable exception) {
                turnOffReadiness(exception);
            }

            @Override
            public void handleTopologyRecoveryException(Connection conn, Channel ch, TopologyRecoveryException exception) {
                turnOffReadiness(exception);
            }

            private void turnOffReadiness(Throwable exception){
                switch (connectionName) {
                    case PUBLISH_CONNECTION_NAME:
                        publishMetrics.getReadinessMonitor().disable();
                        break;
                    case CONSUME_CONNECTION_NAME:
                        consumeMetrics.getReadinessMonitor().disable();
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + connectionName);
                }
                LOGGER.debug("Set RabbitMQ readiness to false. RabbitMQ error", exception);
            }
        });

        factory.setConnectionRecoveryTriggeringCondition(shutdownSignal -> !connectionIsClosed.get() && getConnectionRecoveryTriggeringCondition(connectionName, connectionManagerConfiguration, onFailedRecoveryConnection));

        factory.setRecoveryDelayHandler(recoveryAttempts -> getRecoveryDelay(connectionName, connectionManagerConfiguration));



        HealthMetrics tmpMetrics;
        switch (connectionName) {
            case PUBLISH_CONNECTION_NAME:
                tmpMetrics = publishMetrics;
                factory.setSharedExecutor(publishSharedExecutor);
                break;
            case CONSUME_CONNECTION_NAME:
                tmpMetrics = consumeMetrics;
                factory.setSharedExecutor(consumeSharedExecutor);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + connectionName);
        }
        Connection tmpConnection;
        try {
            tmpConnection = factory.newConnection();
            tmpMetrics.getReadinessMonitor().enable();
            LOGGER.debug("Set RabbitMQ readiness to true");
        } catch (IOException | TimeoutException e) {
            tmpMetrics.getReadinessMonitor().disable();
            LOGGER.debug("Set RabbitMQ readiness to false. Can not create connection {}", connectionName, e);
            throw new IllegalStateException(String.format("Failed to create RabbitMQ %s connection using configuration", connectionName), e);
        }

        attachBlockedListener(tmpConnection, connectionName);
        attachRecoveryListener(tmpConnection, connectionName);
        return tmpConnection;
    }

    private static void attachBlockedListener(Connection connection, String connectionName) {
        connection.addBlockedListener(new BlockedListener() {
            @Override
            public void handleBlocked(String reason) {
                LOGGER.warn("RabbitMQ blocked {} connection: {}", connectionName, reason);
            }

            @Override
            public void handleUnblocked() {
                LOGGER.warn("RabbitMQ unblocked {} connection", connectionName);
            }
        });
    }

    private void attachRecoveryListener(Connection connection, String connectionName) {
        if (connection instanceof Recoverable) {
            Recoverable recoverableConnection = (Recoverable)connection;
            switch (connectionName) {
                case PUBLISH_CONNECTION_NAME:
                    recoverableConnection.addRecoveryListener(publishRecoveryListener);
                    break;
                case CONSUME_CONNECTION_NAME:
                    recoverableConnection.addRecoveryListener(consumehRecoveryListener);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + connectionName);
            }
            LOGGER.debug("Recovery listener was added to {} connection.", connectionName);
        } else {
            throw new IllegalStateException(connectionName + " connection does not implement Recoverable. Can not add RecoveryListener to it");
        }
    }

    private RecoveryListener getRecoveryListener(String connectionName) {
        return new RecoveryListener() {
            @Override
            public void handleRecovery(Recoverable recoverable) {
                LOGGER.debug("Count tries to recovery {} connection reset to 0", connectionName);
                switch (connectionName) {
                    case PUBLISH_CONNECTION_NAME:
                        publishConnectionRecoveryAttempts.set(0);
                        publishMetrics.getReadinessMonitor().enable();
                        break;
                    case CONSUME_CONNECTION_NAME:
                        consumeConnectionRecoveryAttempts.set(0);
                        consumeMetrics.getReadinessMonitor().enable();
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + connectionName);
                }
                LOGGER.debug("Set RabbitMQ readiness to true");
            }

            @Override
            public void handleRecoveryStarted(Recoverable recoverable) {
            }
        };
    }

    private boolean getConnectionRecoveryTriggeringCondition(String connectionName, ConnectionManagerConfiguration connectionManagerConfiguration,
            Runnable onFailedRecoveryConnection) {
        int tmpCountTriesToRecovery;

        switch (connectionName) {
            case PUBLISH_CONNECTION_NAME:
                tmpCountTriesToRecovery = publishConnectionRecoveryAttempts.get();
                break;
            case CONSUME_CONNECTION_NAME:
                tmpCountTriesToRecovery = consumeConnectionRecoveryAttempts.get();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + connectionName);
        }

        if (tmpCountTriesToRecovery < connectionManagerConfiguration.getMaxRecoveryAttempts()) {
            LOGGER.info("Try to recovery {} connection to RabbitMQ. Count tries = {}", connectionName, tmpCountTriesToRecovery + 1);
            return true;
        }
        LOGGER.error("Can not connect to RabbitMQ. Count tries = {}", tmpCountTriesToRecovery);
        if (onFailedRecoveryConnection == null) {
            // TODO: we should stop the execution of the application. Don't use System.exit!!!
            throw new IllegalStateException("Cannot recover connection to RabbitMQ");
        }
        onFailedRecoveryConnection.run();
        return false;
    }

    private int getRecoveryDelay(String connectionName, ConnectionManagerConfiguration connectionManagerConfiguration) {
        int tmpCountTriesToRecovery;
        switch (connectionName) {
        case PUBLISH_CONNECTION_NAME:
            tmpCountTriesToRecovery = publishConnectionRecoveryAttempts.getAndIncrement();
            break;
        case CONSUME_CONNECTION_NAME:
            tmpCountTriesToRecovery = consumeConnectionRecoveryAttempts.getAndIncrement();
            break;
        default:
            throw new IllegalStateException("Unexpected value: " + connectionName);
        }

        int recoveryDelay = connectionManagerConfiguration.getMinConnectionRecoveryTimeout()
                + (connectionManagerConfiguration.getMaxRecoveryAttempts() > 1
                ? (connectionManagerConfiguration.getMaxConnectionRecoveryTimeout() - connectionManagerConfiguration.getMinConnectionRecoveryTimeout())
                / (connectionManagerConfiguration.getMaxRecoveryAttempts() - 1)
                * tmpCountTriesToRecovery
                : 0);

        LOGGER.info("Recovery delay for '{}' try = {}", tmpCountTriesToRecovery, recoveryDelay);
        return recoveryDelay;
    }

    private void closeConnection(Connection connection, String connectionName, int closeTimeout) {
        if (connection.isOpen()) {
            try {
                // We close the connection and don't close channels
                // because when a channel's connection is closed, so is the channel
                connection.close(closeTimeout);
            } catch (IOException e) {
                LOGGER.error("Cannot close {} connection", connectionName, e);
            }
        }
    }

    private static class RabbitMqSubscriberMonitor implements SubscriberMonitor {

        private final ChannelHolder holder;
        private final String tag;
        private final CancelAction action;

        public RabbitMqSubscriberMonitor(ChannelHolder holder, String tag,
                                         CancelAction action) {
            this.holder = holder;
            this.tag = tag;
            this.action = action;
        }

        @Override
        public void unsubscribe() throws Exception {
            holder.withLock(false, channel -> action.execute(channel, tag));
        }
    }

    private interface CancelAction {
        void execute(Channel channel, String tag) throws IOException;
    }

    private static class PinId {
        private final String routingKey;
        private final String queue;

        public static PinId forRoutingKey(String routingKey) {
            return new PinId(routingKey, null);
        }

        public static PinId forQueue(String queue) {
            return new PinId(null, queue);
        }

        private PinId(String routingKey, String queue) {
            if (routingKey == null && queue == null) {
                throw new NullPointerException("Either routingKey or queue must be set");
            }
            this.routingKey = routingKey;
            this.queue = queue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            PinId pinId = (PinId) o;

            return new EqualsBuilder().append(routingKey, pinId.routingKey).append(queue, pinId.queue).isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(routingKey).append(queue).toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                    .append("routingKey", routingKey)
                    .append("queue", queue)
                    .toString();
        }
    }

    private static class ChannelHolder {
        private final Lock lock = new ReentrantLock();
        private final Supplier<Channel> supplier;
        private final BiConsumer<ShutdownNotifier, Boolean> reconnectionChecker;
        private Channel channel;

        public ChannelHolder(
                Supplier<Channel> supplier,
                BiConsumer<ShutdownNotifier, Boolean> reconnectionChecker
        ) {
            this.supplier = Objects.requireNonNull(supplier, "'Supplier' parameter");
            this.reconnectionChecker = Objects.requireNonNull(reconnectionChecker, "'Reconnection checker' parameter");
        }

        public void withLock(ChannelConsumer consumer) throws IOException {
            withLock(true, consumer);
        }

        public void withLock(boolean waitForRecovery, ChannelConsumer consumer) throws IOException {
            lock.lock();
            try {
                consumer.consume(getChannel(waitForRecovery));
            } finally {
                lock.unlock();
            }
        }

        public <T> T mapWithLock(ChannelMapper<T> mapper) throws IOException {
            lock.lock();
            try {
                return mapper.map(getChannel());
            } finally {
                lock.unlock();
            }
        }
        private Channel getChannel() {
            return getChannel(true);
        }

        private Channel getChannel(boolean waitForRecovery) {
            if (channel == null) {
                channel = supplier.get();
            }
            reconnectionChecker.accept(channel, waitForRecovery);
            return channel;
        }
    }

    private interface ChannelMapper<T> {
        T map(Channel channel) throws IOException;
    }

    private interface ChannelConsumer {
        void consume(Channel channel) throws IOException;
    }
}

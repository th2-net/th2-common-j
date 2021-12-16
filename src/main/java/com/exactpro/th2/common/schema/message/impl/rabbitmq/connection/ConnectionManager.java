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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConnectionManager implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private static final String PUBLISH_CONNECTION_NAME = "publish";
    private static final String CONSUME_CONNECTION_NAME = "consume";

    private final Connection publishConnection;
    private final Connection consumeConnection;
    private final Map<PinId, ChannelHolder> channelsByPin = new ConcurrentHashMap<>();
    private final AtomicBoolean connectionIsClosed = new AtomicBoolean(false);
    private final ConnectionManagerConfiguration configuration;
    private final String subscriberName;
    private final AtomicInteger nextSubscriberId = new AtomicInteger(1);
    private final ExecutorService sharedExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("rabbitmq-shared-pool-%d")
            .build());

    private final Map<String, HealthMetrics> connectionNameToMetrics = Map.of(
            PUBLISH_CONNECTION_NAME, new HealthMetrics("amqp_" + PUBLISH_CONNECTION_NAME + "_connection"),
            CONSUME_CONNECTION_NAME, new HealthMetrics("amqp_" + CONSUME_CONNECTION_NAME + "_connection")
    );

    public ConnectionManagerConfiguration getConfiguration() {
        return configuration;
    }

    public ConnectionManager(
            @NotNull RabbitMQConfiguration rabbitMQConfiguration,
            @NotNull ConnectionManagerConfiguration connectionManagerConfiguration
    ) {
        requireNonNull(rabbitMQConfiguration, "RabbitMQ configuration cannot be null");
        this.configuration = requireNonNull(connectionManagerConfiguration, "Connection manager configuration can not be null");

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

        if (connectionManagerConfiguration.getConnectionTimeout() >  0) {
            factory.setConnectionTimeout(connectionManagerConfiguration.getConnectionTimeout());
        }

        factory.setAutomaticRecoveryEnabled(true);
        factory.setSharedExecutor(sharedExecutor);
        publishConnection = createConnection(factory, PUBLISH_CONNECTION_NAME);
        consumeConnection = createConnection(factory, CONSUME_CONNECTION_NAME);
    }

    private Connection createConnection(ConnectionFactory factory, String connectionName) {
        HealthMetrics metrics = connectionNameToMetrics.get(connectionName);
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
                metrics.getReadinessMonitor().disable();
                LOGGER.debug("RabbitMQ error", exception);
            }
        });

        factory.setRecoveryDelayHandler(recoveryAttempts -> {
                    if (connectionIsClosed.get()) {
                        throw new IllegalStateException("Connection is already closed");
                    }
                    int attemptNumber = recoveryAttempts + 1;
                    if (attemptNumber > configuration.getMaxRecoveryAttempts() && metrics.getLivenessMonitor().isEnabled()) {
                        metrics.getLivenessMonitor().disable();
                        LOGGER.error(
                                "Can not connect to RabbitMQ for {} connection after {} attempt(s), will try again",
                                connectionName,
                                configuration.getMaxRecoveryAttempts()
                        );
                    }
                    int recoveryDelay = configuration.getMinConnectionRecoveryTimeout()
                            + (configuration.getMaxRecoveryAttempts() > 1
                                ? (configuration.getMaxConnectionRecoveryTimeout() - configuration.getMinConnectionRecoveryTimeout())
                                    / (configuration.getMaxRecoveryAttempts() - 1)
                                    * recoveryAttempts
                                : 0);
                    LOGGER.info(
                            "Attempt #{} to recover {} connection to RabbitMQ. Next attempt will be after {} ms",
                            attemptNumber,
                            connectionName,
                            recoveryDelay
                    );
                    return recoveryDelay;
                }
        );

        Connection connection;
        try {
            connection = factory.newConnection(connectionName);
            metrics.enable();
        } catch (IOException | TimeoutException e) {
            metrics.disable();
            LOGGER.debug("Can not create {} connection", connectionName, e);
            throw new IllegalStateException(format("Failed to create RabbitMQ %s connection using configuration", connectionName), e);
        }

        connection.addBlockedListener(new BlockedListener() {
            @Override
            public void handleBlocked(String reason) throws IOException {
                LOGGER.warn("RabbitMQ blocked connection: {}", reason);
            }

            @Override
            public void handleUnblocked() throws IOException {
                LOGGER.warn("RabbitMQ unblocked connection");
            }
        });

        if (connection instanceof Recoverable) {
            Recoverable recoverableConnection = (Recoverable) connection;
            recoverableConnection.addRecoveryListener(getRecoveryListener(metrics));
            LOGGER.debug("Recovery listener was added to {} connection.", connectionName);
        } else {
            throw new IllegalStateException(format("%s connection does not implement Recoverable. Can not add RecoveryListener to it", connectionName));
        }
        return connection;
    }

    private RecoveryListener getRecoveryListener(HealthMetrics metrics) {
        return new RecoveryListener() {
            @Override
            public void handleRecovery(Recoverable recoverable) {
                metrics.enable();
            }

            @Override
            public void handleRecoveryStarted(Recoverable recoverable) {
            }
        };
    }

    public boolean isOpen() {
        return (publishConnection.isOpen() || consumeConnection.isOpen())
                && !connectionIsClosed.get();
    }

    @Override
    public void close() {
        if (connectionIsClosed.getAndSet(true)) {
            return;
        }
        closeConnection(publishConnection);
        closeConnection(consumeConnection);
        shutdownSharedExecutor(sharedExecutor);
    }

    private void closeConnection(Connection connection) {
        if (connection.isOpen()) {
            try {
                // We close the connection and don't close channels
                // because when a channel's connection is closed, so is the channel
                connection.close(configuration.getConnectionCloseTimeout());
            } catch (IOException e) {
                LOGGER.error("Cannot close {} connection", connection.getClientProvidedName(), e);
            }
        }
    }

    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body) throws IOException {
        ChannelHolder holder = getChannelFor(PinId.forRoutingKey(routingKey), publishConnection);
        holder.withLock(channel -> {
            channel.basicPublish(exchange, routingKey, props, body);
        });
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

    private void shutdownSharedExecutor(ExecutorService sharedExecutor) {
        sharedExecutor.shutdown();
        try {
            if (!sharedExecutor.awaitTermination(configuration.getConnectionCloseTimeout(), TimeUnit.MILLISECONDS)) {
                LOGGER.error("Executor is not terminated during {} millis", configuration.getConnectionCloseTimeout());
                List<Runnable> runnables = sharedExecutor.shutdownNow();
                LOGGER.error("{} task(s) was(were) not finished", runnables.size());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ChannelHolder getChannelFor(PinId pinId, Connection connection) {
        return channelsByPin.computeIfAbsent(pinId, ignore -> {
            LOGGER.trace("Creating channel holder for {}", pinId);
            return new ChannelHolder(
                    () -> createChannel(connection),
                    (notifier, waitForRecovery) -> waitForConnectionRecovery(notifier, waitForRecovery, connection.getClientProvidedName())
            );
        });
    }

    private Channel createChannel(Connection connection) {
        waitForConnectionRecovery(connection, connection.getClientProvidedName());

        try {
            Channel channel = connection.createChannel();
            requireNonNull(channel, () -> format("No channels are available in the %s connection. Max channel number: %d", connection.getClientProvidedName(), connection.getChannelMax()));
            channel.basicQos(configuration.getPrefetchCount());
            channel.addReturnListener(ret ->
                    LOGGER.warn("Can not router message to exchange '{}', routing key '{}'. Reply code '{}' and text = {}", ret.getExchange(), ret.getRoutingKey(), ret.getReplyCode(), ret.getReplyText()));
            return channel;
        } catch (IOException e) {
            throw new IllegalStateException("Can not create channel", e);
        }
    }

    private void waitForConnectionRecovery(ShutdownNotifier notifier, String connectionName) {
        waitForConnectionRecovery(notifier, true, connectionName);
    }

    private void waitForConnectionRecovery(ShutdownNotifier notifier, boolean waitForRecovery, String connectionName) {
        if (isConnectionRecovery(notifier)) {
            if (waitForRecovery) {
                waitForRecovery(notifier, connectionName);
            } else {
                LOGGER.warn("Skip waiting for {} connection recovery", connectionName);
            }
        }

        if (connectionIsClosed.get()) {
            throw new IllegalStateException("Connection is already closed");
        }
    }

    private void waitForRecovery(ShutdownNotifier notifier, String connectionName) {
        connectionNameToMetrics.get(connectionName).getReadinessMonitor().disable();
        LOGGER.warn("Start waiting for {} connection recovery", connectionName);
        while (isConnectionRecovery(notifier)) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                LOGGER.error("Wait for {} connection recovery was interrupted", connectionName, e);
                break;
            }
        }
        LOGGER.info("Stop waiting for {} connection recovery", connectionName);
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
            this.supplier = requireNonNull(supplier, "'Supplier' parameter");
            this.reconnectionChecker = requireNonNull(reconnectionChecker, "'Reconnection checker' parameter");
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

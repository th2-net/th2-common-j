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

import com.exactpro.th2.common.grpc.MessageGroupBatch;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.impl.OnlyOnceConfirmation;
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
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownNotifier;
import com.rabbitmq.client.TopologyRecoveryException;

import static com.exactpro.th2.common.schema.message.MessageRouterUtils.toShortDebugString;

public class ConnectionManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private final Connection connection;
    private final Map<PinId, ChannelHolder> channelsByPin = new ConcurrentHashMap<>();
    private final AtomicInteger connectionRecoveryAttempts = new AtomicInteger(0);
    private final AtomicBoolean connectionIsClosed = new AtomicBoolean(false);
    private final ConnectionManagerConfiguration configuration;
    private final String subscriberName;
    private final AtomicInteger nextSubscriberId = new AtomicInteger(1);
    private final ExecutorService sharedExecutor;
    private final ScheduledExecutorService channelChecker = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("channel-checker-%d")
            .build());

    private final HealthMetrics metrics = new HealthMetrics(this);

    private final RecoveryListener recoveryListener = new RecoveryListener() {
        @Override
        public void handleRecovery(Recoverable recoverable) {
            LOGGER.debug("Count tries to recovery connection reset to 0");
            connectionRecoveryAttempts.set(0);
            metrics.getReadinessMonitor().enable();
            LOGGER.debug("Set RabbitMQ readiness to true");
        }

        @Override
        public void handleRecoveryStarted(Recoverable recoverable) {}
    };

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

        if (connectionManagerConfiguration.getConnectionTimeout() >  0) {
            factory.setConnectionTimeout(connectionManagerConfiguration.getConnectionTimeout());
        }

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
                LOGGER.debug("Set RabbitMQ readiness to false. RabbitMQ error", exception);
            }
        });

        factory.setAutomaticRecoveryEnabled(true);
        factory.setConnectionRecoveryTriggeringCondition(shutdownSignal -> {
            if (connectionIsClosed.get()) {
                return false;
            }

            int tmpCountTriesToRecovery = connectionRecoveryAttempts.get();

            if (tmpCountTriesToRecovery < connectionManagerConfiguration.getMaxRecoveryAttempts()) {
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

                    int recoveryDelay = connectionManagerConfiguration.getMinConnectionRecoveryTimeout()
                            + (connectionManagerConfiguration.getMaxRecoveryAttempts() > 1
                                ? (connectionManagerConfiguration.getMaxConnectionRecoveryTimeout() - connectionManagerConfiguration.getMinConnectionRecoveryTimeout())
                                    / (connectionManagerConfiguration.getMaxRecoveryAttempts() - 1)
                                    * tmpCountTriesToRecovery
                                : 0);

                    LOGGER.info("Recovery delay for '{}' try = {}", tmpCountTriesToRecovery, recoveryDelay);
                    return recoveryDelay;
                }
        );
        sharedExecutor = Executors.newFixedThreadPool(configuration.getWorkingThreads(), new ThreadFactoryBuilder()
                .setNameFormat("rabbitmq-shared-pool-%d")
                .build());
        factory.setSharedExecutor(sharedExecutor);

        try {
            this.connection = factory.newConnection();
            metrics.getReadinessMonitor().enable();
            LOGGER.debug("Set RabbitMQ readiness to true");
        } catch (IOException | TimeoutException e) {
            metrics.getReadinessMonitor().disable();
            LOGGER.debug("Set RabbitMQ readiness to false. Can not create connection", e);
            throw new IllegalStateException("Failed to create RabbitMQ connection using configuration", e);
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
            recoverableConnection.addRecoveryListener(recoveryListener);
            LOGGER.debug("Recovery listener was added to connection.");
        } else {
            throw new IllegalStateException("Connection does not implement Recoverable. Can not add RecoveryListener to it");
        }
    }

    public boolean isOpen() {
        return connection.isOpen() && !connectionIsClosed.get();
    }

    @Override
    public void close() {
        if (connectionIsClosed.getAndSet(true)) {
            LOGGER.info("Connection manager already closed");
            return;
        }

        LOGGER.info("Closing connection manager");

        int closeTimeout = configuration.getConnectionCloseTimeout();

        for(ChannelHolder channel : channelsByPin.values()) {
            try {
                if(channel.isPublisher) {
                    channel.channel.waitForConfirmsOrDie(closeTimeout);
                } else {
                    channel.channel.close();
                }
            } catch (Exception e) {
                LOGGER.error("Error while closing channel", e);
            }
        }

        if (connection.isOpen()) {
            try {
                connection.close(closeTimeout);
            } catch (IOException e) {
                LOGGER.error("Cannot close connection", e);
            }
        }

        shutdownExecutor(sharedExecutor, closeTimeout, "rabbit-shared");
        shutdownExecutor(channelChecker, closeTimeout, "channel-checker");
    }

    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body) throws IOException {
        ChannelHolder holder = getChannelFor(PinId.forRoutingKey(routingKey), true);
        holder.withLock(channel -> channel.basicPublish(exchange, routingKey, props, body));
    }

    public SubscriberMonitor basicConsume(String queue, ManualAckDeliveryCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
        ChannelHolder holder = getChannelFor(PinId.forQueue(queue), false);
        String tag = holder.mapWithLock(channel ->
                channel.basicConsume(queue, false, subscriberName + "_" + nextSubscriberId.getAndIncrement(), (tagTmp, delivery) -> {
                    try {
                        Envelope envelope = delivery.getEnvelope();
                        long deliveryTag = envelope.getDeliveryTag();
                        String routingKey = envelope.getRoutingKey();
                        LOGGER.trace("Received delivery {} from queue={} routing_key={}", deliveryTag, queue, routingKey);

                        Confirmation confirmation = OnlyOnceConfirmation.wrap("from " + routingKey + " to " + queue, () -> holder.withLock(ch -> {
                            try {
                                basicAck(ch, deliveryTag);
                            } finally {
                                holder.release(() -> metrics.getReadinessMonitor().enable());
                            }
                        }));

                        holder.withLock(() -> holder.acquireAndSubmitCheck(() ->
                                channelChecker.schedule(() -> {
                                    holder.withLock(() -> {
                                        LOGGER.warn("The confirmation for delivery {} in queue={} routing_key={} was not invoked within the specified delay",
                                                deliveryTag, queue, routingKey);
                                        if (holder.reachedPendingLimit()) {
                                            metrics.getReadinessMonitor().disable();
                                        }
                                    });
                                    return false; // to cast to Callable
                                }, configuration.getConfirmationTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        ));
                        deliverCallback.handle(tagTmp, delivery, confirmation);
                    } catch (IOException | RuntimeException e) {
                        LOGGER.error("Cannot handle delivery for tag {}: {}", tagTmp, e.getMessage(), e);
                    }
                }, cancelCallback));

        return new RabbitMqSubscriberMonitor(holder, tag, this::basicCancel);
    }

    boolean isReady() {
        return metrics.getReadinessMonitor().isEnabled();
    }

    boolean isAlive() {
        return metrics.getLivenessMonitor().isEnabled();
    }

    private void basicCancel(Channel channel, String consumerTag) throws IOException {
        channel.basicCancel(consumerTag);
    }

    private void shutdownExecutor(ExecutorService executor, int closeTimeout, String name) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(closeTimeout, TimeUnit.MILLISECONDS)) {
                LOGGER.error("Executor {} is not terminated during {} millis", name, closeTimeout);
                List<Runnable> runnables = executor.shutdownNow();
                LOGGER.error("{} task(s) was(were) not finished in executor {}", runnables.size(), name);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ChannelHolder getChannelFor(PinId pinId, boolean isPublisher) {
        return channelsByPin.computeIfAbsent(pinId, ignore -> {
            LOGGER.trace("Creating channel holder for {}", pinId);
            return new ChannelHolder(() -> createChannel(), this::waitForConnectionRecovery, configuration.getPrefetchCount(), isPublisher);
        });
    }

    private Channel createChannel() {
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
    private static void basicAck(Channel channel, long deliveryTag) throws IOException {
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
        private final int maxCount;
        private final boolean isPublisher;
        @GuardedBy("lock")
        private int pending;
        @GuardedBy("lock")
        private Future<?> check;
        @GuardedBy("lock")
        private Channel channel;

        public ChannelHolder(
                Supplier<Channel> supplier,
                BiConsumer<ShutdownNotifier, Boolean> reconnectionChecker,
                int maxCount,
                boolean isPublisher
        ) {
            this.supplier = Objects.requireNonNull(supplier, "'Supplier' parameter");
            this.reconnectionChecker = Objects.requireNonNull(reconnectionChecker, "'Reconnection checker' parameter");
            this.maxCount = maxCount;
            this.isPublisher = isPublisher;
        }

        public void withLock(ChannelConsumer consumer) throws IOException {
            withLock(true, consumer);
        }

        public void withLock(Runnable action) {
            lock.lock();
            try {
                action.run();
            } finally {
                lock.unlock();
            }
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

        /**
         * Decreases the number of unacked messages.
         * If the number of unacked messages is less than {@link #maxCount}
         * the <b>onWaterMarkDecreased</b> action will be called.
         * The future created in {@link #acquireAndSubmitCheck(Supplier)} method will be canceled
         * @param onWaterMarkDecreased
         * the action that will be executed when the number of unacked messages is less than {@link #maxCount} and there is a future to cancel
         */
        public void release(Runnable onWaterMarkDecreased) {
            lock.lock();
            try {
                pending--;
                if (pending < maxCount && check != null) {
                    check.cancel(true);
                    check = null;
                    onWaterMarkDecreased.run();
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Increases the number of unacked messages.
         * If the number of unacked messages is higher than or equal to {@link #maxCount}
         * the <b>futureSupplier</b> will be invoked to create a task
         * that either will be executed or canceled when number of unacked message will be less that {@link #maxCount}
         * @param futureSupplier
         * creates a future to track the task that should be executed until the number of unacked message is not less than {@link #maxCount}
         */
        public void acquireAndSubmitCheck(Supplier<Future<?>> futureSupplier) {
            lock.lock();
            try {
                pending++;
                if (reachedPendingLimit() && check == null) {
                    check = futureSupplier.get();
                }
            } finally {
                lock.unlock();
            }
        }

        public boolean reachedPendingLimit() {
            lock.lock();
            try {
                return pending >= maxCount;
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
                try {
                    if(isPublisher) {
                        channel.confirmSelect();
                        channel.addConfirmListener(
                                (deliveryTag, multiple) -> {
                                    if(LOGGER.isTraceEnabled()) {
                                        if(!multiple) {
                                            LOGGER.trace("Message with delivery tag {} confirmed. Ids:\n{}", deliveryTag);
                                        } else {
                                            LOGGER.trace("Messages prior to delivery tag {} confirmed. Id:\n{}", deliveryTag);
                                        }
                                    }
                                },
                                (deliveryTag, multiple) -> {
                                    if(LOGGER.isErrorEnabled()) {
                                        if(!multiple) {
                                            LOGGER.error("Message with delivery tag {} was nacked. Ids:\n{}", deliveryTag);
                                        } else {
                                            LOGGER.error("Messages prior to delivery tag {} was nacked. Id:\n{}", deliveryTag);
                                        }
                                    }
                                }
                        );
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Can not create channel", e);
                }
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

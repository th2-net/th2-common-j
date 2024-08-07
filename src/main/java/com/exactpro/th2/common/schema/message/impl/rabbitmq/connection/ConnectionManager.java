/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RetryingDelay;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownNotifier;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.TopologyRecoveryException;
import com.rabbitmq.client.impl.AMQImpl;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public abstract class ConnectionManager implements AutoCloseable {
    public static final String EMPTY_ROUTING_KEY = "";
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private final Connection connection;
    protected final Map<PinId, ChannelHolder> channelsByPin = new ConcurrentHashMap<>();
    private final AtomicReference<State> connectionState = new AtomicReference<>(State.OPEN);
    private final ConnectionManagerConfiguration configuration;
    protected final ScheduledExecutorService channelChecker;
    protected final HealthMetrics metrics = new HealthMetrics(this);

    private final RecoveryListener recoveryListener = new RecoveryListener() {
        @Override
        public void handleRecovery(Recoverable recoverable) {
            metrics.getReadinessMonitor().enable();
            LOGGER.info("Recovery finished. Set RabbitMQ readiness to true");
            metrics.getLivenessMonitor().enable();
        }

        @Override
        public void handleRecoveryStarted(Recoverable recoverable) {
            LOGGER.warn("Recovery started...");
        }
    };

    public ConnectionManagerConfiguration getConfiguration() {
        return configuration;
    }

    private enum State { OPEN, CLOSING, CLOSED }

    public ConnectionManager(
            @NotNull String connectionName,
            @NotNull RabbitMQConfiguration rabbitMQConfiguration,
            @NotNull ConnectionManagerConfiguration connectionManagerConfiguration,
            @NotNull ExecutorService sharedExecutor,
            @NotNull ScheduledExecutorService channelChecker
    ) {
        Objects.requireNonNull(rabbitMQConfiguration, "RabbitMQ configuration cannot be null");
        this.configuration = Objects.requireNonNull(connectionManagerConfiguration, "Connection manager configuration can not be null");
        Objects.requireNonNull(sharedExecutor, "Shared executor can not be null");
        this.channelChecker = Objects.requireNonNull(channelChecker, "channelChecker executor can not be null");

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

        if (connectionManagerConfiguration.getHeartbeatIntervalSeconds() > ConnectionManagerConfiguration.DEFAULT_HB_INTERVAL_SECONDS) {
            factory.setRequestedHeartbeat(connectionManagerConfiguration.getHeartbeatIntervalSeconds());
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

            private void turnOffReadiness(Throwable exception) {
                metrics.getReadinessMonitor().disable();
                LOGGER.debug("Set RabbitMQ readiness to false. RabbitMQ error", exception);
            }
        });

        factory.setConnectionRecoveryTriggeringCondition(shutdownSignal -> connectionState.get() != State.CLOSED);

        factory.setRecoveryDelayHandler(recoveryAttempts -> {
            int minTime = configuration.getMinConnectionRecoveryTimeout();
            int maxTime = configuration.getMaxConnectionRecoveryTimeout();
            int maxRecoveryAttempts = configuration.getMaxRecoveryAttempts();
            int deviationPercent = configuration.getRetryTimeDeviationPercent();

            LOGGER.debug("Try to recovery connection to RabbitMQ. Count tries = {}", recoveryAttempts);
            int recoveryDelay = RetryingDelay.getRecoveryDelay(recoveryAttempts, minTime, maxTime, maxRecoveryAttempts, deviationPercent);
            if (recoveryAttempts >= maxRecoveryAttempts && metrics.getLivenessMonitor().isEnabled()) {
                LOGGER.info("Set RabbitMQ liveness to false. Can't recover connection");
                metrics.getLivenessMonitor().disable();
            }

            LOGGER.info("Recovery delay for '{}' try = {}", recoveryAttempts, recoveryDelay);
            return recoveryDelay;
        });

        factory.setSharedExecutor(sharedExecutor);

        try {
            connection = factory.newConnection(connectionName);
            LOGGER.info("Created RabbitMQ connection {} [{}]", connection, connection.hashCode());
            addShutdownListenerToConnection(connection);
            addBlockedListenersToConnection(connection);
            addRecoveryListenerToConnection(connection);
            metrics.getReadinessMonitor().enable();
            LOGGER.debug("Set RabbitMQ readiness to true");
        } catch (IOException | TimeoutException e) {
            metrics.getReadinessMonitor().disable();
            LOGGER.debug("Set RabbitMQ readiness to false. Can not create connection", e);
            throw new IllegalStateException("Failed to create RabbitMQ connection using configuration", e);
        }
    }

    private final static List<String> recoverableErrors = List.of(
            "reply-text=NOT_FOUND",
            "reply-text=PRECONDITION_FAILED"
    );

    private void addShutdownListenerToChannel(Channel channel, Boolean withRecovery) {
        channel.addShutdownListener(cause -> {
            LOGGER.debug("Closing the channel: ", cause);
            if (!cause.isHardError() && cause.getReference() instanceof Channel) {
                Channel channelCause = (Channel) cause.getReference();
                Method reason = cause.getReason();
                if (reason instanceof AMQImpl.Channel.Close) {
                    var castedReason = (AMQImpl.Channel.Close) reason;
                    if (castedReason.getReplyCode() != 200) {
                        StringBuilder errorBuilder = new StringBuilder("RabbitMQ soft error occurred: ");
                        castedReason.appendArgumentDebugStringTo(errorBuilder);
                        errorBuilder.append(" on channel ");
                        errorBuilder.append(channelCause);
                        String errorString = errorBuilder.toString();
                        LOGGER.warn(errorString);
                        if (withRecovery) {
                            var pinIdToChannelHolder = getChannelHolderByChannel(channel);
                            if (pinIdToChannelHolder != null && recoverableErrors.stream().anyMatch(errorString::contains)) {
                                var holder = pinIdToChannelHolder.getValue();
                                if (holder.isChannelSubscribed(channel)) {
                                    var pinId = pinIdToChannelHolder.getKey();
                                    recoverSubscriptionsOfChannel(pinId, channel, holder);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    private @Nullable Map.Entry<PinId, ChannelHolder> getChannelHolderByChannel(Channel channel) {
        return channelsByPin
                .entrySet()
                .stream()
                .filter(entry -> channel == entry.getValue().channel)
                .findAny()
                .orElse(null);
    }

    protected abstract void recoverSubscriptionsOfChannel(@NotNull final PinId pinId, Channel channel, @NotNull final ChannelHolder holder);

    private void addShutdownListenerToConnection(Connection conn) {
        conn.addShutdownListener(cause -> {
            LOGGER.debug("Closing the connection: ", cause);
            if (cause.isHardError() && cause.getReference() instanceof Connection) {
                Connection connectionCause = (Connection) cause.getReference();
                Method reason = cause.getReason();
                if (reason instanceof AMQImpl.Connection.Close) {
                    var castedReason = (AMQImpl.Connection.Close) reason;
                    if (castedReason.getReplyCode() != 200) {
                        StringBuilder errorBuilder = new StringBuilder("RabbitMQ hard error occupied: ");
                        castedReason.appendArgumentDebugStringTo(errorBuilder);
                        errorBuilder.append(" on connection ");
                        errorBuilder.append(connectionCause);
                        LOGGER.warn(errorBuilder.toString());
                    }
                }
            }
        });
    }

    private void addRecoveryListenerToConnection(Connection conn) {
        if (conn instanceof Recoverable) {
            Recoverable recoverableConnection = (Recoverable) conn;
            recoverableConnection.addRecoveryListener(recoveryListener);
            LOGGER.debug("Recovery listener was added to connection.");
        } else {
            throw new IllegalStateException("Connection does not implement Recoverable. Can not add RecoveryListener to it");
        }
    }

    protected abstract BlockedListener createBlockedListener();

    private void addBlockedListenersToConnection(Connection conn) {
        conn.addBlockedListener(createBlockedListener());
    }

    public boolean isOpen() {
        return connection.isOpen() && connectionState.get() == State.OPEN;
    }

    @Override
    public void close() {
        if (!connectionState.compareAndSet(State.OPEN, State.CLOSING)) {
            LOGGER.info("Connection manager already closed");
            return;
        }

        LOGGER.info("Closing connection manager");
        int closeTimeout = configuration.getConnectionCloseTimeout();

        for (Map.Entry<PinId, ChannelHolder> entry: channelsByPin.entrySet()) {
            PinId id = entry.getKey();
            ChannelHolder channelHolder = entry.getValue();
            try {
                if (channelHolder.hasUnconfirmedMessages()) {
                    if (channelHolder.noConfirmationWillBeReceived()) {
                        LOGGER.warn("Some messages were not confirmed by broken in channel {} and were not republished. Try to republish messages", id);
                        channelHolder.publishUnconfirmedMessages();
                    }
                    LOGGER.info("Waiting for messages confirmation in channel {}", id);
                    try {
                        channelHolder.awaitConfirmations(closeTimeout, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ignored) {
                        LOGGER.warn("Waiting for messages confirmation in channel {} was interrupted", id);
                        Thread.currentThread().interrupt();
                    }
                }
                if (channelHolder.hasUnconfirmedMessages()) {
                    LOGGER.error("RabbitMQ channel for pin {} has unconfirmed messages on close", id);
                }
                channelHolder.channel.abort();
            } catch (IOException e) {
                LOGGER.error("Cannot close channel for pin {}", id, e);
            }
        }

        connectionState.set(State.CLOSED);

        if (connection.isOpen()) {
            try {
                connection.close(closeTimeout);
            } catch (IOException e) {
                LOGGER.error("Failed to close connection", e);
            }
        }
    }

    boolean isReady() {
        return metrics.getReadinessMonitor().isEnabled();
    }

    boolean isAlive() {
        return metrics.getLivenessMonitor().isEnabled();
    }

    protected ChannelHolderOptions configurationToOptions() {
        return new ChannelHolderOptions(
                configuration.getPrefetchCount(),
                configuration.getEnablePublisherConfirmation(),
                configuration.getMaxInflightPublicationsBytes()
        );
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

    protected static final class SubscriptionCallbacks {
        final ManualAckDeliveryCallback deliverCallback;
        final CancelCallback cancelCallback;

        public SubscriptionCallbacks(ManualAckDeliveryCallback deliverCallback, CancelCallback cancelCallback) {
            this.deliverCallback = deliverCallback;
            this.cancelCallback = cancelCallback;
        }
    }

   protected ChannelHolder getOrCreateChannelFor(PinId pinId) {
        return channelsByPin.computeIfAbsent(pinId, ignore -> {
            LOGGER.trace("Creating channel holder for {}", pinId);
            return new ChannelHolder(this::createChannel, this::waitForConnectionRecovery, configurationToOptions());
        });
    }

    protected ChannelHolder getOrCreateChannelFor(PinId pinId, SubscriptionCallbacks subscriptionCallbacks) {
        return channelsByPin.computeIfAbsent(pinId, ignore -> {
            LOGGER.trace("Creating channel holder with callbacks for {}", pinId);
            return new ChannelHolder(() -> createChannelWithOptionalRecovery(true), this::waitForConnectionRecovery, configurationToOptions(), subscriptionCallbacks);
        });
    }

    protected void putChannelFor(PinId pinId, ChannelHolder holder) {
        ChannelHolder previous = channelsByPin.putIfAbsent(pinId, holder);
        if (previous != null) {
            throw new IllegalStateException("Channel holder for the '" + pinId + "' pinId has been already registered");
        }
    }

    protected Channel createChannel() {
        return createChannelWithOptionalRecovery(false);
    }

    private Channel createChannelWithOptionalRecovery(Boolean withRecovery) {
        waitForConnectionRecovery(connection);

        try {
            Channel channel = connection.createChannel();
            Objects.requireNonNull(channel, () -> "No channels are available in the connection. Max channel number: " + connection.getChannelMax());
            channel.basicQos(configuration.getPrefetchCount());
            addShutdownListenerToChannel(channel, withRecovery);
            channel.addReturnListener(ret ->
                    LOGGER.warn("Can not router message to exchange '{}', routing key '{}'. Reply code '{}' and text = {}", ret.getExchange(), ret.getRoutingKey(), ret.getReplyCode(), ret.getReplyText()));
            LOGGER.info("Created new RabbitMQ channel {} via connection {}", channel.getChannelNumber(), connection.hashCode());
            return channel;
        } catch (IOException e) {
            throw new IllegalStateException("Can not create channel", e);
        }
    }

    private void waitForConnectionRecovery(ShutdownNotifier notifier) {
        waitForConnectionRecovery(notifier, true);
    }

    protected void waitForConnectionRecovery(ShutdownNotifier notifier, boolean waitForRecovery) {
        if (isConnectionRecovery(notifier)) {
            if (waitForRecovery) {
                waitForRecovery(notifier);
            } else {
                LOGGER.warn("Skip waiting for connection recovery");
            }
        }

        if (connectionState.get() == State.CLOSED) {
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
        return !(notifier instanceof AutorecoveringChannel) && !notifier.isOpen()
                && connectionState.get() != State.CLOSED;
    }

    protected interface CancelAction {
        void execute(Channel channel, String tag) throws IOException;
    }

    protected static class PinId {
        private final String exchange;
        private final String routingKey;
        protected final String queue;

        public static PinId forRoutingKey(String exchange, String routingKey) {
            return new PinId(exchange, routingKey, null);
        }

        public static PinId forQueue(String queue) {
            return new PinId(null, null, queue);
        }

        private PinId(String exchange, String routingKey, String queue) {
            if ((exchange == null || routingKey == null) && queue == null) {
                throw new NullPointerException("Either exchange and routingKey or queue must be set");
            }
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.queue = queue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            PinId pinId = (PinId) o;

            return new EqualsBuilder()
                    .append(exchange, pinId.exchange)
                    .append(routingKey, pinId.routingKey)
                    .append(queue, pinId.queue).isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(exchange)
                    .append(routingKey)
                    .append(queue).toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                    .append("exchange", exchange)
                    .append("routingKey", routingKey)
                    .append("queue", queue)
                    .toString();
        }
    }

    protected static class ChannelHolderOptions {
        private final int maxCount;
        private final boolean enablePublisherConfirmation;
        private final int maxInflightRequestsBytes;

        private ChannelHolderOptions(int maxCount, boolean enablePublisherConfirmation, int maxInflightRequestsBytes) {
            this.maxCount = maxCount;
            this.enablePublisherConfirmation = enablePublisherConfirmation;
            this.maxInflightRequestsBytes = maxInflightRequestsBytes;
        }

        public int getMaxCount() {
            return maxCount;
        }

        public boolean isEnablePublisherConfirmation() {
            return enablePublisherConfirmation;
        }

        public int getMaxInflightRequestsBytes() {
            return maxInflightRequestsBytes;
        }
    }

    protected static class ChannelHolder {
        protected final Lock lock = new ReentrantLock();
        private final Supplier<Channel> supplier;
        private final BiConsumer<ShutdownNotifier, Boolean> reconnectionChecker;
        private final ChannelHolderOptions options;
        private final SubscriptionCallbacks subscriptionCallbacks;
        @GuardedBy("lock")
        private int pending;
        @GuardedBy("lock")
        private Future<?> check;
        @GuardedBy("lock") // or by `subscribingLock` for `basicConsume` channels
        private Channel channel;
        private final Lock subscribingLock = new ReentrantLock();
        @GuardedBy("subscribingLock")
        private boolean isSubscribed;
        @GuardedBy("lock")
        private final Deque<PublicationHolder> redeliveryQueue = new ArrayDeque<>();

        private final PublisherConfirmationListener publishConfirmationListener;

        public ChannelHolder(
                Supplier<Channel> supplier,
                BiConsumer<ShutdownNotifier, Boolean> reconnectionChecker,
                ChannelHolderOptions options
        ) {
            this(supplier, reconnectionChecker, options, null);
        }

        public ChannelHolder(
                Supplier<Channel> supplier,
                BiConsumer<ShutdownNotifier, Boolean> reconnectionChecker,
                ChannelHolderOptions options,
                SubscriptionCallbacks subscriptionCallbacks

        ) {
            this.supplier = Objects.requireNonNull(supplier, "'Supplier' parameter");
            this.reconnectionChecker = Objects.requireNonNull(reconnectionChecker, "'Reconnection checker' parameter");
            this.options = options;
            this.subscriptionCallbacks = subscriptionCallbacks;
            publishConfirmationListener = new PublisherConfirmationListener(
                    options.isEnablePublisherConfirmation(),
                    options.getMaxInflightRequestsBytes()
            );
        }

        public boolean hasUnconfirmedMessages() {
            return publishConfirmationListener.hasUnconfirmedMessages();
        }

        public boolean noConfirmationWillBeReceived() {
            return publishConfirmationListener.isNoConfirmationWillBeReceived();
        }

        public boolean awaitConfirmations(long timeout, TimeUnit timeUnit) throws InterruptedException {
            return publishConfirmationListener.awaitConfirmations(timeout, timeUnit);
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

        public void retryingPublishWithLock(ConnectionManagerConfiguration configuration, byte[] payload, ChannelPublisher publisher) throws IOException, InterruptedException {
            lock.lock();
            try {
                Iterator<RetryingDelay> iterator = configuration.createRetryingDelaySequence().iterator();
                Channel tempChannel = getChannel(true);
                // add current message to the end to unify logic for sending current and redelivered messages
                redeliveryQueue.addLast(new PublicationHolder(publisher, payload));
                while (true) {
                    if (publishConfirmationListener.isNoConfirmationWillBeReceived()) {
                        LOGGER.warn("Connection was closed on channel. No delivery confirmation will be received. Drain message to redelivery");
                        publishConfirmationListener.transferUnconfirmedTo(redeliveryQueue);
                    }
                    PublicationHolder currentPayload = redeliveryQueue.pollFirst();
                    if (!redeliveryQueue.isEmpty()) {
                        LOGGER.warn("Redelivery queue size: {}", redeliveryQueue.size());
                    }
                    if (currentPayload == null) {
                        break;
                    }
                    long msgSeq = tempChannel.getNextPublishSeqNo();
                    try {
                        publishConfirmationListener.put(msgSeq, currentPayload);
                        if (publishConfirmationListener.isNoConfirmationWillBeReceived()) {
                            // will drain message to queue on next iteration
                            continue;
                        }
                        currentPayload.publish(tempChannel);
                        if (redeliveryQueue.isEmpty()) {
                            break;
                        }
                    } catch (IOException | ShutdownSignalException e) {
                        var currentValue = iterator.next();
                        var recoveryDelay = currentValue.getDelay();
                        LOGGER.warn("Retrying publishing #{}, waiting for {}ms. Reason: {}", currentValue.getTryNumber(), recoveryDelay, e);
                        TimeUnit.MILLISECONDS.sleep(recoveryDelay);
                        // cleanup after failure
                        publishConfirmationListener.remove(msgSeq);
                        redeliveryQueue.addFirst(currentPayload);

                        // We should not recover the channel if its connection is closed
                        // If we do that the channel will be also auto recovered by RabbitMQ client
                        // during connection recovery, and we will get two new channels instead of one closed.
                        if (!tempChannel.isOpen() && tempChannel.getConnection().isOpen()) {
                            // once channel is recreated there won't be any confirmation received
                            // so we should redeliver all inflight requests
                            publishConfirmationListener.transferUnconfirmedTo(redeliveryQueue);
                            tempChannel = recreateChannel();
                        }
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        public <T> T retryingConsumeWithLock(ChannelMapper<T> mapper, ConnectionManagerConfiguration configuration) throws InterruptedException, IOException {
            lock.lock();
            try {
                Iterator<RetryingDelay> iterator = null;
                IOException exception;
                Channel tempChannel = null;
                boolean isChannelClosed = false;
                while (true) {
                    subscribingLock.lock();
                    try {
                        if (tempChannel == null) {
                            tempChannel = getChannel();
                        } else if (isChannelClosed) {
                            tempChannel = recreateChannel();
                        }

                        var tag = mapper.map(tempChannel);
                        isSubscribed = true;
                        return tag;
                    } catch (IOException e) {
                        var reason = tempChannel.getCloseReason();
                        isChannelClosed = reason != null;

                        // We should not retry in this case because we never will be able to connect to the queue if we
                        // receive this error. This error happens if we try to subscribe to an exclusive queue that was
                        // created by another connection.
                        if (isChannelClosed && reason.getMessage().contains("reply-text=RESOURCE_LOCKED")) {
                            throw e;
                        }
                        exception = e;
                    } finally {
                        subscribingLock.unlock();
                    }
                    iterator = handleAndSleep(configuration, iterator, "Retrying consume", exception);
                }
            } finally {
                lock.unlock();
            }
        }

        public @Nullable SubscriptionCallbacks getCallbacksForRecovery(Channel channelToRecover) {
            if (!isSubscribed) {
                // if unsubscribe() method was invoked after channel failure
                LOGGER.warn("Channel's consumer was unsubscribed.");
                return null;
            }

            if (channel != channelToRecover) {
                // this can happens if basicConsume() method was invoked by client code after channel failure
                LOGGER.warn("Channel already recreated.");
                return null;
            }

            // recovery should not be called for `basicPublish` channels
            if (subscriptionCallbacks == null) throw new IllegalStateException("Channel has no consumer");

            return subscriptionCallbacks;
        }

        public void unsubscribeWithLock(String tag, CancelAction action) throws IOException {
            lock.lock();
            try {
                subscribingLock.lock();
                try {
                    action.execute(channel, tag);
                    isSubscribed = false;
                } finally {
                    subscribingLock.unlock();
                }
            } finally {
                lock.unlock();
            }
        }

        @NotNull
        private static Iterator<RetryingDelay> handleAndSleep(
                ConnectionManagerConfiguration configuration,
                Iterator<RetryingDelay> iterator,
                String comment,
                Exception e) throws InterruptedException {
            iterator = iterator == null ? configuration.createRetryingDelaySequence().iterator() : iterator;
            RetryingDelay currentValue = iterator.next();
            int recoveryDelay = currentValue.getDelay();

            LOGGER.warn("{} #{}, waiting for {} ms, then recreating channel. Reason: {}", comment, currentValue.getTryNumber(), recoveryDelay, e);
            TimeUnit.MILLISECONDS.sleep(recoveryDelay);
            return iterator;
        }

        public <T> T mapWithLock(ChannelMapper<T> mapper) throws IOException {
            lock.lock();
            try {
                Channel channel = getChannel();
                return mapper.map(channel);
            } catch (IOException e) {
                LOGGER.error("Operation failure on the {} channel", channel.getChannelNumber(), e);
                throw e;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Decreases the number of unacked messages.
         * If the number of unacked messages is less than {@link ChannelHolderOptions#getMaxCount()}
         * the <b>onWaterMarkDecreased</b> action will be called.
         * The future created in {@link #acquireAndSubmitCheck(Supplier)} method will be canceled
         * @param onWaterMarkDecreased
         * the action that will be executed when the number of unacked messages is less than {@link ChannelHolderOptions#getMaxCount()} and there is a future to cancel
         */
        public void release(Runnable onWaterMarkDecreased) {
            lock.lock();
            try {
                pending--;
                if (pending < options.getMaxCount() && check != null) {
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
         * If the number of unacked messages is higher than or equal to {@link ChannelHolderOptions#getMaxCount()}
         * the <b>futureSupplier</b> will be invoked to create a task
         * that either will be executed or canceled when number of unacked message will be less that {@link ChannelHolderOptions#getMaxCount()}
         * @param futureSupplier
         * creates a future to track the task that should be executed until the number of unacked message is not less than {@link ChannelHolderOptions#getMaxCount()}
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

        public boolean isChannelSubscribed(Channel channel) {
            subscribingLock.lock();
            try {
                return isSubscribed && channel == this.channel;
            } finally {
                subscribingLock.unlock();
            }
        }

        public boolean reachedPendingLimit() {
            lock.lock();
            try {
                return pending >= options.getMaxCount();
            } finally {
                lock.unlock();
            }
        }

        private Channel getChannel() throws IOException {
            return getChannel(true);
        }

        private Channel recreateChannel() throws IOException {
            channel = supplier.get();
            reconnectionChecker.accept(channel, true);
            setupPublisherConfirmation(channel);
            return channel;
        }

        private Channel getChannel(boolean waitForRecovery) throws IOException {
            if (channel == null) {
                channel = supplier.get();
                setupPublisherConfirmation(channel);
            }
            reconnectionChecker.accept(channel, waitForRecovery);
            return channel;
        }

        private void setupPublisherConfirmation(Channel channel) throws IOException {
            if (!options.isEnablePublisherConfirmation()) {
                return;
            }
            channel.confirmSelect();
            channel.addShutdownListener(cause -> publishConfirmationListener.noConfirmationWillBeReceived());
            channel.addConfirmListener(publishConfirmationListener);
        }

        public void publishUnconfirmedMessages() throws IOException {
            lock.lock();
            try {
                Channel channel = getChannel(false);
                if (!channel.isOpen()) {
                    throw new IllegalStateException("channel is not opened to publish unconfirmed messages");
                }
                publishConfirmationListener.transferUnconfirmedTo(redeliveryQueue);
                while (!redeliveryQueue.isEmpty()) {
                    PublicationHolder holder = redeliveryQueue.pollFirst();
                    holder.publish(channel);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private static class PublicationHolder {
        private final ChannelPublisher publisher;
        private final byte[] payload;
        private volatile boolean confirmed;

        private PublicationHolder(ChannelPublisher publisher, byte[] payload) {
            this.publisher = publisher;
            this.payload = payload;
        }

        int size() {
            return payload.length;
        }

        boolean isConfirmed() {
            return confirmed;
        }

        void confirmed() {
            confirmed = true;
        }

        void reset() {
            confirmed = false;
        }

        public void publish(Channel channel) throws IOException {
            publisher.publish(channel, payload);
        }
    }

    private static class PublisherConfirmationListener implements ConfirmListener {
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private final Condition hasSpaceToWriteCondition = lock.writeLock().newCondition();
        private final Condition allMessagesConfirmed = lock.writeLock().newCondition();
        @GuardedBy("lock")
        private final NavigableMap<Long, PublicationHolder> inflightRequests = new TreeMap<>();
        private final int maxInflightRequestsBytes;
        private final boolean enablePublisherConfirmation;
        private final boolean hasLimit;
        @GuardedBy("lock")
        private boolean noConfirmationWillBeReceived;
        @GuardedBy("lock")
        private int inflightBytes;

        private PublisherConfirmationListener(boolean enablePublisherConfirmation, int maxInflightRequestsBytes) {
            if (maxInflightRequestsBytes <= 0 && maxInflightRequestsBytes != ConnectionManagerConfiguration.NO_LIMIT_INFLIGHT_REQUESTS) {
                throw new IllegalArgumentException("invalid maxInflightRequests: " + maxInflightRequestsBytes);
            }
            hasLimit = maxInflightRequestsBytes != ConnectionManagerConfiguration.NO_LIMIT_INFLIGHT_REQUESTS;
            this.maxInflightRequestsBytes = maxInflightRequestsBytes;
            this.enablePublisherConfirmation = enablePublisherConfirmation;
        }

        public void put(long deliveryTag, PublicationHolder payload) throws InterruptedException {
            if (!enablePublisherConfirmation) {
                return;
            }
            lock.writeLock().lock();
            try {
                int payloadSize = payload.size();
                if (hasLimit) {
                    int newSize = inflightBytes + payloadSize;
                    if (newSize > maxInflightRequestsBytes) {
                        LOGGER.warn("blocking because {} inflight requests bytes size is above limit {} bytes for publication channel",
                                newSize, maxInflightRequestsBytes);
                        do {
                            hasSpaceToWriteCondition.await();
                            newSize = inflightBytes + payloadSize;
                        } while (newSize > maxInflightRequestsBytes && !noConfirmationWillBeReceived);
                        if (noConfirmationWillBeReceived) {
                            LOGGER.warn("unblocking because no confirmation will be received and inflight request size will not change");
                        } else {
                            LOGGER.info("unblocking because {} inflight requests bytes size is below limit {} bytes for publication channel",
                                    newSize, maxInflightRequestsBytes);
                        }
                    }
                }
                inflightRequests.put(deliveryTag, payload);
                inflightBytes += payloadSize;
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void remove(long deliveryTag) {
            if (!enablePublisherConfirmation) {
                return;
            }
            lock.writeLock().lock();
            try {
                PublicationHolder holder = inflightRequests.remove(deliveryTag);
                if (holder == null) {
                    return;
                }
                inflightBytes -= holder.size();
                hasSpaceToWriteCondition.signalAll();
                if (inflightRequests.isEmpty()) {
                    inflightBytes = 0;
                    allMessagesConfirmed.signalAll();
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public void handleAck(long deliveryTag, boolean multiple) {
            LOGGER.trace("Delivery ack received for tag {} (multiple:{})", deliveryTag, multiple);
            removeInflightRequests(deliveryTag, multiple);
            LOGGER.trace("Delivery ack processed for tag {} (multiple:{})", deliveryTag, multiple);
        }

        @Override
        public void handleNack(long deliveryTag, boolean multiple) {
            LOGGER.warn("Delivery nack received for tag {} (multiple:{})", deliveryTag, multiple);
            // we cannot do match with nack because this is an internal error in rabbitmq
            // we can try to republish the message but there is no guarantee that the message will be accepted
            removeInflightRequests(deliveryTag, multiple);
            LOGGER.warn("Delivery nack processed for tag {} (multiple:{})", deliveryTag, multiple);
        }

        private void removeInflightRequests(long deliveryTag, boolean multiple) {
            if (!enablePublisherConfirmation) {
                return;
            }
            lock.writeLock().lock();
            try {
                int currentSize = inflightBytes;
                if (multiple) {
                    Map<Long, PublicationHolder> headMap = inflightRequests.headMap(deliveryTag, true);
                    for (Map.Entry<Long, PublicationHolder> entry : headMap.entrySet()) {
                        currentSize -= entry.getValue().size();
                    }
                    headMap.clear();
                } else {
                    long oldestPublication = Objects.requireNonNullElse(inflightRequests.firstKey(), deliveryTag);
                    if (oldestPublication == deliveryTag) {
                        // received the confirmation for oldest publication
                        // check all earlier confirmation that were confirmed but not removed
                        Iterator<Map.Entry<Long, PublicationHolder>> tailIterator =
                                inflightRequests.tailMap(deliveryTag, true).entrySet().iterator();
                        while (tailIterator.hasNext()) {
                            Map.Entry<Long, PublicationHolder> next = tailIterator.next();
                            long key = next.getKey();
                            PublicationHolder holder = next.getValue();
                            if (key > deliveryTag && !holder.isConfirmed()) {
                                break;
                            }
                            currentSize -= holder.size();
                            tailIterator.remove();
                        }
                    } else {
                        // this is not the oldest publication
                        // mark as confirm but wait for oldest to be confirmed
                        var holder = inflightRequests.get(deliveryTag);
                        if (holder != null) {
                            holder.confirmed();
                        }
                    }
                }
                if (inflightBytes != currentSize) {
                    inflightBytes = currentSize;
                    hasSpaceToWriteCondition.signalAll();
                }
                if (inflightRequests.isEmpty()) {
                    inflightBytes = 0;
                    allMessagesConfirmed.signalAll();
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        public boolean hasUnconfirmedMessages() {
            lock.readLock().lock();
            try {
                return !inflightRequests.isEmpty();
            } finally {
                lock.readLock().unlock();
            }
        }

        /**
         * Adds unconfirmed messages to provided queue.
         * Messages will be added to the beginning of the queue.
         * As result, queue will have the oldest messages in the start and newest in the end.
         * </p>
         * {@link #inflightRequests} map will be cleared.
         * </p>
         * {@link #noConfirmationWillBeReceived} will be reset
         * @param redelivery queue to transfer messages
         */
        public void transferUnconfirmedTo(Deque<PublicationHolder> redelivery) {
            lock.writeLock().lock();
            try {
                for (PublicationHolder payload : inflightRequests.descendingMap().values()) {
                    payload.reset();
                    redelivery.addFirst(payload);
                }
                inflightRequests.clear();
                inflightBytes = 0;
                noConfirmationWillBeReceived = false;
                hasSpaceToWriteCondition.signalAll();
                allMessagesConfirmed.signalAll();
            } finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * Indicates that no confirmation will be received for inflight requests.
         * Method {@link #transferUnconfirmedTo(Deque)} should be called to reset the flag
         * and obtain messages for redeliver
         * @return true if channel was closed and no confirmation will be received
         */
        public boolean isNoConfirmationWillBeReceived() {
            lock.readLock().lock();
            try {
                return noConfirmationWillBeReceived;
            } finally {
                lock.readLock().unlock();
            }
        }

        public void noConfirmationWillBeReceived() {
            lock.writeLock().lock();
            try {
                LOGGER.warn("Publication listener was notified that no confirmations will be received");
                noConfirmationWillBeReceived = true;
                // we need to unlock possible locked publisher, so it can check that nothing will be confirmed
                // and retry the publication
                hasSpaceToWriteCondition.signalAll();
            } finally {
                lock.writeLock().unlock();
            }
        }

        public boolean awaitConfirmations(long timeout, TimeUnit timeUnit) throws InterruptedException {
            lock.writeLock().lock();
            try {
                return inflightRequests.isEmpty() || allMessagesConfirmed.await(timeout, timeUnit);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    protected interface ChannelMapper<T> {
        T map(Channel channel) throws IOException;
    }

    protected interface ChannelConsumer {
        void consume(Channel channel) throws IOException;
    }

    protected interface ChannelPublisher {
        void publish(Channel channel, byte[] payload) throws IOException;
    }
}
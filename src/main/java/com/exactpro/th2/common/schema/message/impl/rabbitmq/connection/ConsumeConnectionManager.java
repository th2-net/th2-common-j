package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.TopologyRecoveryException;

public class ConsumeConnectionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeConnectionManager.class);
    private final Connection connection;
    private final AtomicInteger connectionRecoveryAttempts = new AtomicInteger(0);
    private final AtomicBoolean connectionIsClosed = new AtomicBoolean(false);
    private final ConnectionManagerConfiguration configuration;
    private final ExecutorService sharedExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("rabbitmq-shared-pool-%d")
            .build());
    private final HealthMetrics metrics = new HealthMetrics(this);

    public ConsumeConnectionManager(@NotNull ConnectionManagerConfiguration connectionManagerConfiguration,
            Runnable onFailedRecoveryConnection, ConnectionFactory factory) {
        this.configuration = Objects.requireNonNull(connectionManagerConfiguration, "Connection manager configuration can not be null");
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

        factory.setConnectionRecoveryTriggeringCondition(shutdownSignal -> !connectionIsClosed.get() && getConnectionRecoveryTriggeringCondition(onFailedRecoveryConnection));

        factory.setRecoveryDelayHandler(recoveryAttempts -> getRecoveryDelay());

        factory.setSharedExecutor(sharedExecutor);

        try {
            connection = factory.newConnection();
            metrics.getReadinessMonitor().enable();
            LOGGER.debug("Set RabbitMQ readiness to true");
        } catch (IOException | TimeoutException e) {
            metrics.getReadinessMonitor().disable();
            LOGGER.debug("Set RabbitMQ readiness to false. Can not create consume connection", e);
            throw new IllegalStateException("Failed to create RabbitMQ consume connection using configuration", e);
        }

        attachBlockedListener();
        attachRecoveryListener();
    }

    private boolean getConnectionRecoveryTriggeringCondition(Runnable onFailedRecoveryConnection) {
        int tmpCountTriesToRecovery = connectionRecoveryAttempts.get();

        if (tmpCountTriesToRecovery < configuration.getMaxRecoveryAttempts()) {
            LOGGER.info("Try to recovery consume connection to RabbitMQ. Count tries = {}", tmpCountTriesToRecovery + 1);
            return true;
        }
        LOGGER.error("Can not connect to RabbitMQ. Count tries = {}", tmpCountTriesToRecovery);
        if (onFailedRecoveryConnection == null) {
            throw new IllegalStateException("Cannot recover connection to RabbitMQ");
        }
        onFailedRecoveryConnection.run();
        return false;
    }

    private int getRecoveryDelay() {
        int tmpCountTriesToRecovery = connectionRecoveryAttempts.getAndIncrement();

        int recoveryDelay = configuration.getMinConnectionRecoveryTimeout()
                + (configuration.getMaxRecoveryAttempts() > 1
                ? (configuration.getMaxConnectionRecoveryTimeout() - configuration.getMinConnectionRecoveryTimeout())
                / (configuration.getMaxRecoveryAttempts() - 1)
                * tmpCountTriesToRecovery
                : 0);

        LOGGER.info("Recovery delay for '{}' try = {}", tmpCountTriesToRecovery, recoveryDelay);
        return recoveryDelay;
    }

    private void attachBlockedListener() {
        connection.addBlockedListener(new BlockedListener() {
            @Override
            public void handleBlocked(String reason) {
                LOGGER.warn("RabbitMQ blocked consume connection: {}", reason);
            }

            @Override
            public void handleUnblocked() {
                LOGGER.warn("RabbitMQ unblocked consume connection");
            }
        });
    }

    private void attachRecoveryListener() {
        if (connection instanceof Recoverable) {
            Recoverable recoverableConnection = (Recoverable)connection;
            recoverableConnection.addRecoveryListener(getRecoveryListener());
            LOGGER.debug("Recovery listener was added to consume connection.");
        } else {
            throw new IllegalStateException("Consume connection does not implement Recoverable. Can not add RecoveryListener to it");
        }
    }

    private RecoveryListener getRecoveryListener() {
        return new RecoveryListener() {
            @Override
            public void handleRecovery(Recoverable recoverable) {
                LOGGER.debug("Count tries to recovery consume connection reset to 0");
                connectionRecoveryAttempts.set(0);
                metrics.getReadinessMonitor().enable();
                LOGGER.debug("Set RabbitMQ readiness to true");
            }

            @Override
            public void handleRecoveryStarted(Recoverable recoverable) {
            }
        };
    }

    public void shutdownSharedExecutor(int closeTimeout) {
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

    public void closeConnection(int closeTimeout) {
        if (connectionIsClosed.getAndSet(true)) {
            return;
        }
        if (connection.isOpen()) {
            try {
                // We close the connection and don't close channels
                // because when a channel's connection is closed, so is the channel
                connection.close(closeTimeout);
            } catch (IOException e) {
                LOGGER.error("Cannot close consume connection", e);
            }
        }
    }

    public Connection getConnection() {
        return connection;
    }
}

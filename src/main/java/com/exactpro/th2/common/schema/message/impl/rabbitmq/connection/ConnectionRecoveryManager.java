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
import com.exactpro.th2.common.metrics.MetricArbiter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryDelayHandler;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.TopologyRecoveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class ConnectionRecoveryManager implements RecoveryListener, ExceptionHandler, RecoveryDelayHandler, Predicate<ShutdownSignalException> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionRecoveryManager.class);

    private final AtomicInteger connectionRecoveryAttempts = new AtomicInteger(0);
    private final Runnable onFailedRecoveryConnection;
    private final RabbitMQConfiguration configuration;
    private final AtomicBoolean connectionIsClosed;

    private final MetricArbiter.MetricMonitor livenessMonitor = CommonMetrics.registerLiveness(this);
    private final MetricArbiter.MetricMonitor readinessMonitor = CommonMetrics.registerReadiness(this);

    public ConnectionRecoveryManager(RabbitMQConfiguration configuration, Runnable onFailedRecoveryConnection, AtomicBoolean connectionIsClosed) {
        this.configuration = configuration;
        this.onFailedRecoveryConnection = onFailedRecoveryConnection;
        this.connectionIsClosed = connectionIsClosed;
    }

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

    @Override
    public long getDelay(int recoveryAttempts) {
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

    @Override
    public void handleRecovery(Recoverable recoverable) {
        LOGGER.debug("Count tries to recovery connection reset to 0");
        connectionRecoveryAttempts.set(0);
        CommonMetrics.setRabbitMQReadiness(true);
        LOGGER.debug("Set RabbitMQ readiness to true");
    }

    @Override
    public void handleRecoveryStarted(Recoverable recoverable) {}

    @Override
    public boolean test(ShutdownSignalException shutdownSignalException) {
        if (connectionIsClosed.get()) {
            return false;
        }

        int tmpCountTriesToRecovery = connectionRecoveryAttempts.get();

        if (tmpCountTriesToRecovery < configuration.getMaxRecoveryAttempts()) {
            LOGGER.info("Try to recovery connection to RabbitMQ. Count tries = {}", tmpCountTriesToRecovery + 1);
            return true;
        }
        LOGGER.error("Can not connect to RabbitMQ. Count tries = {}", tmpCountTriesToRecovery);
        if (onFailedRecoveryConnection != null) {
            onFailedRecoveryConnection.run();
        }
        livenessMonitor.disable();
        readinessMonitor.disable();
        return false;
    }

    private void turnOffReadiness(Throwable exception){
        readinessMonitor.disable();
        LOGGER.error("RabbitMQ connection error", exception);
    }
}

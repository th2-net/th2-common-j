/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.schema.message.impl.rabbitmq.connection;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownNotifier;

import lombok.val;

public class ConnectionManager implements AutoCloseable {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final Connection connection;
    private final ThreadLocal<Channel> channel = ThreadLocal.withInitial(this::createChannel);
    private final AtomicInteger connectionRecoveryAttempts = new AtomicInteger(0);
    private final AtomicBoolean connectionIsClosed = new AtomicBoolean(false);
    private final RabbitMQConfiguration configuration;
    private final String subscriberName;
    private final AtomicInteger nextSubscriberId = new AtomicInteger(1);

    private final RecoveryListener recoveryListener = new RecoveryListener() {
        @Override
        public void handleRecovery(Recoverable recoverable) {
            logger.debug("Count tries to recovery connection reset to 0");
            connectionRecoveryAttempts.set(0);
        }

        @Override
        public void handleRecoveryStarted(Recoverable recoverable) {}
    };

    public ConnectionManager(@NotNull RabbitMQConfiguration rabbitMQConfiguration, Runnable onFailedRecoveryConnection) {
        this.configuration = Objects.requireNonNull(rabbitMQConfiguration, "RabbitMQ configuration cannot be null");

        if (StringUtils.isNotEmpty(rabbitMQConfiguration.getSubscriberName())) {
            subscriberName = "rabbit_mq_subscriber." + System.currentTimeMillis();
            logger.info("Subscribers will use default name: {}", subscriberName);
        } else {
            subscriberName = rabbitMQConfiguration.getSubscriberName() + "." + System.currentTimeMillis();
        }

        val factory = new ConnectionFactory();
        val virtualHost = rabbitMQConfiguration.getvHost();
        val username = rabbitMQConfiguration.getUsername();
        val password = rabbitMQConfiguration.getPassword();

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

        factory.setAutomaticRecoveryEnabled(true);
        factory.setConnectionRecoveryTriggeringCondition(shutdownSignal -> {
            if (connectionIsClosed.get()) {
                return false;
            }

            int tmpCountTriesToRecovery = connectionRecoveryAttempts.get();

            if (tmpCountTriesToRecovery < rabbitMQConfiguration.getMaxRecoveryAttempts()) {
                logger.info("Try to recovery connection to RabbitMQ. Count tries = {}", tmpCountTriesToRecovery + 1);
                return true;
            }
            logger.error("Can not connect to RabbitMQ. Count tries = {}", tmpCountTriesToRecovery);
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
                            + (rabbitMQConfiguration.getMaxConnectionRecoveryTimeout() - rabbitMQConfiguration.getMinConnectionRecoveryTimeout())
                            / rabbitMQConfiguration.getMaxRecoveryAttempts()
                            * tmpCountTriesToRecovery;

                    logger.info("Recovery delay for '{}' try = {}", tmpCountTriesToRecovery, recoveryDelay);
                    return recoveryDelay;
                }
        );

        try {
            this.connection = factory.newConnection();
        } catch (IOException | TimeoutException e) {
            throw new IllegalStateException("Failed to create RabbitMQ connection using following configuration: " + rabbitMQConfiguration, e);
        }

        if (this.connection instanceof Recoverable) {
            Recoverable recoverableConnection = (Recoverable) this.connection;
            recoverableConnection.addRecoveryListener(recoveryListener);
            logger.debug("Recovery listener was added to connection.");
        } else {
            throw new IllegalStateException("Connection is not implements Recoverable. Can not add RecoveryListener to one");
        }
    }

    public boolean isOpen() {
        return connection.isOpen() && !connectionIsClosed.get();
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
    }

    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body) throws IOException {
        Channel channel = this.channel.get();
        waitForConnectionRecovery(channel);
        channel.basicPublish(exchange, routingKey, props, body);
    }

    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
        Channel channel = this.channel.get();
        waitForConnectionRecovery(channel);
        return channel.basicConsume(queue, autoAck, subscriberName + "_" + nextSubscriberId.getAndIncrement(), deliverCallback, cancelCallback);
    }

    public void basicCancel(String consumerTag) throws IOException {
        Channel channel = this.channel.get();
        waitForConnectionRecovery(channel);
        channel.basicCancel(consumerTag);
    }

    private Channel createChannel() {
        waitForConnectionRecovery(connection);

        try {
            return connection.createChannel();
        } catch (IOException e) {
            throw new IllegalStateException("Can not create channel", e);
        }
    }

    private void waitForConnectionRecovery(ShutdownNotifier notifier) {
        while (!notifier.isOpen() && !connectionIsClosed.get()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                logger.error("Wait for connection recovery was interrupted", e);
                break;
            }
        }

        if (connectionIsClosed.get()) {
            throw new IllegalStateException("Connection is already closed");
        }
    }
}

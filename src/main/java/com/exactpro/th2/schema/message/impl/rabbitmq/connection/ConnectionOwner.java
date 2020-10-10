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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import lombok.val;

public class ConnectionOwner implements AutoCloseable {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final AtomicReference<Connection> connection = new AtomicReference<>(null);
    private final AtomicInteger countTriesToRecoveryConnection = new AtomicInteger(0);
    private final RabbitMQConfiguration configuration;
    private final String subscriberName;

    public ConnectionOwner(@NotNull RabbitMQConfiguration rabbitMQConfiguration, Runnable onFailedRecoveryConnection) {
        this.configuration = Objects.requireNonNull(rabbitMQConfiguration, "RabbitMQ configuration cannot be null");

        subscriberName = rabbitMQConfiguration.getSubscriberName();

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
            if (countTriesToRecoveryConnection.get() < rabbitMQConfiguration.getCountTryRecoveryConnection()) {
                logger.info("Try to recovery connection to RabbitMQ. Count tries = {}", countTriesToRecoveryConnection.get() + 1);
                return true;
            }
            logger.error("Can not connect to RabbitMQ. Count tries = {}", countTriesToRecoveryConnection.get());
            if (onFailedRecoveryConnection != null) {
                onFailedRecoveryConnection.run();
            } else {
                // TODO: we should stop the execution of the application. Don't use System.exit!!!
                throw new IllegalStateException("Can not recovery connection to RabbitMQ");
            }
            return false;
        });

        factory.setRecoveryDelayHandler(recoveryAttempts -> {
                    int recoveryDelay = rabbitMQConfiguration.getMinTimeoutForRecoveryConnection()
                            + (rabbitMQConfiguration.getMaxTimeoutForRecoveryConnection() - rabbitMQConfiguration.getMinTimeoutForRecoveryConnection())
                            / rabbitMQConfiguration.getCountTryRecoveryConnection()
                            * countTriesToRecoveryConnection.get();

                    logger.info("Recovery delay for '{}' try = {}", countTriesToRecoveryConnection.incrementAndGet(), recoveryDelay);
                    return recoveryDelay;
                }
        );

        this.connection.updateAndGet(connection -> {
            try {
                return factory.newConnection();
            } catch (IOException | TimeoutException e) {
                throw new IllegalStateException("Failed to create RabbitMQ connection using following configuration: " + rabbitMQConfiguration, e);
            }
        });
    }

    public Channel createChannel() throws IOException {
        Connection tmp = this.connection.get();
        if (tmp == null || !tmp.isOpen()) {
            throw new IllegalStateException("Connection is close");
        }

        return tmp.createChannel();
    }

    public String getSubscriberName() {
        return subscriberName;
    }

    @Override
    public void close() throws IllegalStateException {
        connection.updateAndGet(connection -> {
            if (connection != null && connection.isOpen()) {
                try {
                    connection.close(configuration.getConnectionCloseTimeout());
                } catch (IOException e) {
                    throw new IllegalStateException("Can not close connection");
                }
            }
            return null;
        });
    }
}

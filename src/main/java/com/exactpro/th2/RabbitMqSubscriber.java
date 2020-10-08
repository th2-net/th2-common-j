/*****************************************************************************
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
 *****************************************************************************/
package com.exactpro.th2;

import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQHost;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQPass;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQPort;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQUser;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQVhost;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class RabbitMqSubscriber implements Closeable {
    private static final int CLOSE_TIMEOUT = 1_000;

    private static final int COUNT_TRY_TO_CONNECT = 5;
    private static final int MIN_SHUTDOWN_TIMEOUT = 10_000;
    private static final int MAX_SHUTDOWN_TIMEOUT = 60_000;

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqSubscriber.class);
    private final String exchangeName;
    private final String[] routes;
    private final DeliverCallback deliverCallback;
    private final CancelCallback cancelCallback;
    private Connection connection;
    private Channel channel;

    public RabbitMqSubscriber(String exchangeName,
                              DeliverCallback deliverCallback,
                              CancelCallback cancelCallback,
                              String... routes) {
        this.exchangeName = Objects.requireNonNull(exchangeName, "exchange name is null");
        this.deliverCallback = deliverCallback;
        this.cancelCallback = cancelCallback;
        this.routes = Objects.requireNonNull(routes, "queueNames is null");
    }

    public void startListening() throws IOException, TimeoutException {
        startListening(getEnvRabbitMQHost(), getEnvRabbitMQVhost(), getEnvRabbitMQPort(), getEnvRabbitMQUser(), getEnvRabbitMQPass(), null, null);
    }

    public void startListening(String host, String vHost, int port, String username, String password) throws IOException, TimeoutException {
        this.startListening(host, vHost, port, username, password, null, null);
    }

    public void startListening(String host, String vHost, int port, String username, String password, Runnable onFailedRecoveryConnection) throws IOException, TimeoutException {
        this.startListening(host, vHost, port, username, password, null, onFailedRecoveryConnection);
    }

    public void startListening(String host, String vHost, int port, String username, String password, @Nullable String subscriberName) throws IOException, TimeoutException {
        this.startListening(host, vHost, port, username, password, subscriberName, null);
    }

    public void startListening(String host, String vHost, int port, String username, String password, @Nullable String subscriberName, Runnable onFailedRecoveryConnection) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        if (StringUtils.isNotEmpty(vHost)) {
            factory.setVirtualHost(vHost);
        }
        factory.setPort(port);
        if (StringUtils.isNotEmpty(username)) {
            factory.setUsername(username);
        }
        if (StringUtils.isNotEmpty(password)) {
            factory.setPassword(password);
        }

        final AtomicInteger countTriesToRecoveryConnection = new AtomicInteger(0);

        factory.setAutomaticRecoveryEnabled(true);
        factory.setConnectionRecoveryTriggeringCondition(s -> {
            if (countTriesToRecoveryConnection.get() < COUNT_TRY_TO_CONNECT) {
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
                    int recoveryDelay = MIN_SHUTDOWN_TIMEOUT
                            + (MAX_SHUTDOWN_TIMEOUT - MIN_SHUTDOWN_TIMEOUT)
                            / COUNT_TRY_TO_CONNECT
                            * countTriesToRecoveryConnection.get();

                    logger.info("Recovery delay for '{}' try = {}", countTriesToRecoveryConnection.incrementAndGet(), recoveryDelay);
                    return recoveryDelay;
                }
        );

        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(exchangeName, "direct");
        subscribeToRoutes(exchangeName, subscriberName, routes);
    }

    @Override
    public void close() throws IOException {
        if (connection != null && connection.isOpen()) {
            connection.close(CLOSE_TIMEOUT);
        }
    }

    private void subscribeToRoutes(String exchangeName, String subscriberName, String[] routes)
            throws IOException {
        for (String route : routes) {
            DeclareOk declareResult = subscriberName == null ? channel.queueDeclare() : channel.queueDeclare(subscriberName + "." + System.currentTimeMillis(), false, true, true, Collections.emptyMap());
            String queue = declareResult.getQueue();
            channel.queueBind(queue, exchangeName, route);
            channel.basicConsume(queue, true, deliverCallback, consumerTag -> logger.info("consuming cancelled for {}", consumerTag));
            logger.info("Start listening exchangeName='{}', routingKey='{}', queue='{}'", exchangeName, route, queue);
        }
    }
}

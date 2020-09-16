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

package com.exactpro.th2.schema.message.impl.rabbitmq;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ConnectionHelper {

    private static final int CLOSE_TIMEOUT = 1_000;
    private static final Logger logger = LoggerFactory.getLogger(ConnectionHelper.class);

    private static ConnectionHelper connectionHelper = null;

    private final Connection connection;
    private final AtomicInteger countConnection = new AtomicInteger(0);

    ConnectionHelper(RabbitMQConfiguration configuration) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(configuration.getHost());

        String virtualHost = configuration.getvHost();
        if (isNotEmpty(virtualHost)) {
            factory.setVirtualHost(virtualHost);
        }

        factory.setPort(configuration.getPort());

        String username = configuration.getUsername();
        if (isNotEmpty(username)) {
            factory.setUsername(username);
        }

        String password = configuration.getPassword();
        if (isNotEmpty(password)) {
            factory.setPassword(password);
        }

        connection = factory.newConnection(newFixedThreadPool(configuration.getCountThread(), new ThreadFactoryBuilder().setNameFormat("rabbit-mq-connection-%d").build()));
        logger.debug("Create connection to RabbitMQ. Count threads = {}", configuration.getCountThread());
    }

    public Connection getConnection() {
        countConnection.incrementAndGet();
        logger.trace("Increase count connection");
        return connection;
    }

    public void close() throws IOException {
        logger.trace("Decrement count connection");

        if (countConnection.decrementAndGet() == 0) {
            if (connection != null && connection.isOpen()) {
                logger.trace("Try to close connection to RabbitMQ");
                try {
                    connection.close(CLOSE_TIMEOUT);
                    logger.debug("Success close connection");
                } catch (IOException e) {
                    logger.debug("Can not close connection", e);
                    throw e;
                }

            }
        }
    }

    public static ConnectionHelper getInstance(RabbitMQConfiguration configuration) throws IOException, TimeoutException {
        synchronized (ConnectionHelper.class) {
            if (connectionHelper == null) {
                connectionHelper = new ConnectionHelper(configuration);
            }

            return connectionHelper;
        }
    }
}

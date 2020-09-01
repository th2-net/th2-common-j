/*****************************************************************************
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
 *****************************************************************************/

package com.exactpro.th2.schema.message.impl.rabbitmq;

import java.io.IOException;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.message.MessageSender;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public abstract class AbstractRabbitSender<T> implements MessageSender<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int CLOSE_TIMEOUT = 1_000;

    private ConnectionFactory factory = new ConnectionFactory();
    private Connection connection = null;
    private Channel channel = null;

    private String sendQueue = null;
    private String exchangeName = null;

    public void init(@NotNull RabbitMQConfiguration configuration, @NotNull String exchangeName, @NotNull String sendQueue) {
        this.sendQueue = sendQueue;
        this.exchangeName = Objects.requireNonNull(exchangeName, "Exchange name can not be null");

        factory.setHost(configuration.getHost());

        String virtualHost = configuration.getvHost();
        if (StringUtils.isNotEmpty(virtualHost)) {
            factory.setVirtualHost(virtualHost);
        }

        factory.setPort(configuration.getPort());

        String username = configuration.getUsername();
        if (StringUtils.isNotEmpty(username)) {
            factory.setUsername(username);
        }

        String password = configuration.getPassword();
        if (StringUtils.isNotEmpty(password)) {
            factory.setPassword(password);
        }
    }

    @Override
    public void start() throws Exception {
        if (sendQueue == null || exchangeName == null) {
            throw new IllegalStateException("Sender can not start. Sender did not init");
        }

        if (connection == null) {
            connection = factory.newConnection();
        }

        if (channel == null) {
            channel = connection.createChannel();
            channel.exchangeDeclare(exchangeName, "direct");
        }
    }

    @Override
    public boolean isClose() {
        return connection == null || !connection.isOpen();
    }

    @Override
    public void send(T value) throws IOException {
        if (channel == null) {
            throw new IllegalStateException("Can not send. Sender did not started");
        }

        synchronized (channel) {
            channel.basicPublish(exchangeName, sendQueue, null, valueToBytes(value));
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Message sent to exchangeName='{}', routing key='{}': '{}'",
                    exchangeName, sendQueue, toShortDebugString(value));
        }
    }

    @Override
    public void close() throws IOException {
        if (connection != null && connection.isOpen()) {
            connection.close(CLOSE_TIMEOUT);
        }
    }

    protected String toShortDebugString(T value) {
        return value.toString();
    }

    protected abstract byte[] valueToBytes(T value);
}

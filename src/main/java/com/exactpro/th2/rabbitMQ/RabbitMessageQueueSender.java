/*
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
 */
package com.exactpro.th2.rabbitMQ;

import java.io.IOException;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.message.IMessageSender;
import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.rabbitMQ.configuration.IRabbitMQConfiguration;
import com.google.protobuf.TextFormat;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMessageQueueSender implements IMessageSender<Message> {

    private static final int CLOSE_TIMEOUT = 1_000;

    private final Logger logger = LoggerFactory.getLogger(this.getClass() + "@" + this.hashCode());

    private ConnectionFactory factory = new ConnectionFactory();
    private Connection connection = null;
    private Channel channel = null;

    private String sendQueue = null;
    private String exchangeName = null;

    public void init(@NotNull IRabbitMQConfiguration configuration, @NotNull String sendQueue) {
        this.sendQueue = sendQueue;
        this.exchangeName = Objects.requireNonNull(configuration.getExchangeName(), "Exchange name can not be null");

        factory.setHost(configuration.getHost());

        String virtualHost = configuration.getVirtualHost();
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
            throw new IllegalStateException("Sender did not init");
        }

        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(exchangeName, "direct");
    }

    @Override
    public void send(Message message) throws IOException {
        if (channel == null) {
            throw new IllegalStateException("Sender did not init");
        }

        synchronized (channel) {
            channel.basicPublish(exchangeName, sendQueue, null, message.toByteArray());
        }
        if(logger.isInfoEnabled()) { logger.info("Sent '{}':'"+ TextFormat.shortDebugString(message) + '\'', sendQueue); }
    }

    @Override
    public void close() throws IOException {
        if (connection != null && connection.isOpen()) {
            connection.close(CLOSE_TIMEOUT);
        }
    }
}

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
package com.exactpro.th2;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.common.grpc.Message;
import com.google.protobuf.TextFormat;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @deprecated please use {@link RabbitMqMessageBatchSender}
 */
public class RabbitMqMessageSender {
    private final Logger logger = LoggerFactory.getLogger(RabbitMqMessageSender.class.getName());
    private final Connection connection;
    private final Channel channel;
    private final String exchande;
    private final String sendMsgQueueName;

    /**
     * @deprecated please use {@link #RabbitMqMessageSender(RabbitMQConfiguration, String, String)}
     */
    @Deprecated
    public RabbitMqMessageSender(RabbitMQConfiguration rabbitMQ, String connectivityId, String exchangeName, String sendMsgQueueName) {
        this(rabbitMQ, exchangeName, sendMsgQueueName);
    }

    /**
     * @deprecated please use {@link #RabbitMqMessageSender(String, String, String, String, int, String, String)}
     */
    @Deprecated
    public RabbitMqMessageSender(String connectivityId, String exchangeName, String sendMsgQueueName,
            String host, String vHost, int port, String username, String password) {
        this(exchangeName, sendMsgQueueName, host, vHost, port, username, password);
    }

    public RabbitMqMessageSender(RabbitMQConfiguration rabbitMQ, String exchangeName, String sendMsgQueueName) {
        this(exchangeName, sendMsgQueueName, rabbitMQ.getHost(), rabbitMQ.getVirtualHost(), rabbitMQ.getPort(), rabbitMQ.getUsername(), rabbitMQ.getPassword());
    }

    public RabbitMqMessageSender(String exchangeName, String sendMsgQueueName,
                                 String host, String vHost, int port, String username, String password) {
        this.exchande = exchangeName;
        this.sendMsgQueueName = sendMsgQueueName;
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
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(exchangeName, "direct");
        } catch (TimeoutException | IOException e) {
            logger.error("RabbitMQ error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert In/Out message to proto and send to the queue
     */
    public void send(Message message) throws IOException {
        synchronized (channel) {
            channel.basicPublish(exchande, sendMsgQueueName, null, message.toByteArray());
        }
        if(logger.isInfoEnabled()) { logger.info("Sent '{}':'"+ TextFormat.shortDebugString(message) + '\'', sendMsgQueueName); }
    }

    public void close() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }
}

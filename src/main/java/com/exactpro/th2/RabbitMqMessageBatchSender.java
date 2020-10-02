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
import com.exactpro.th2.common.grpc.MessageBatch;
import com.google.protobuf.TextFormat;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMqMessageBatchSender {
    private final Logger logger = LoggerFactory.getLogger(RabbitMqMessageBatchSender.class.getName());
    private final Connection connection;
    private final Channel channel;
    private final String exchande;
    private final String sendMsgQueueName;

    public RabbitMqMessageBatchSender(RabbitMQConfiguration rabbitMQ, String exchangeName, String sendMsgQueueName) {
        this(exchangeName, sendMsgQueueName, rabbitMQ.getHost(), rabbitMQ.getVirtualHost(), rabbitMQ.getPort(), rabbitMQ.getUsername(), rabbitMQ.getPassword());
    }

    public RabbitMqMessageBatchSender(String exchangeName, String sendMsgQueueName,
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

    public void send(MessageBatch batch) throws IOException {
        synchronized (channel) {
            channel.basicPublish(exchande, sendMsgQueueName, null, batch.toByteArray());
        }
        if(logger.isInfoEnabled()) { logger.info("Sent '{}':'"+ TextFormat.shortDebugString(batch) + '\'', sendMsgQueueName); }
    }

    public void close() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }
}

/******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/
package com.exactpro.th2;

import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQHost;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQPass;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQPort;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQUser;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQVhost;

import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.Value;
import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.google.protobuf.TextFormat;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMqMessageSender {
    private final Logger logger = LoggerFactory.getLogger(RabbitMqMessageSender.class.getName());
    private final Connection connection;
    private final Channel channel;
    private final IMessageToProtoConverter converter;
    private final String connectivityId;
    private final String exchande;
    private final String sendMsgQueueName;

    public RabbitMqMessageSender(RabbitMQConfiguration rabbitMQ, String connectivityId, String exchangeName, String sendMsgQueueName) {
        this(connectivityId, exchangeName, sendMsgQueueName, rabbitMQ.getHost(), rabbitMQ.getVirtualHost(), rabbitMQ.getPort(), rabbitMQ.getUsername(), rabbitMQ.getPassword());
    }

    public RabbitMqMessageSender(String connectivityId, String exchangeName, String sendMsgQueueName,
                                 String host, String vHost, int port, String username, String password) {
        this.connectivityId = connectivityId;
        this.exchande = exchangeName;
        this.sendMsgQueueName = sendMsgQueueName;
        this.converter = new IMessageToProtoConverter();
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

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        RabbitMqSubscriber subscriber = new RabbitMqSubscriber("test_remote_rabbit", "test_queue");
        subscriber.startListening(getEnvRabbitMQHost(), getEnvRabbitMQVhost(), getEnvRabbitMQPort(), getEnvRabbitMQUser(), getEnvRabbitMQPass(), "test_rabbit_mq_sender");
        // subscriber.startListening("th2", 5672, "th2", "ahw3AeWa");
        // subscriber.startListening("10.64.66.114", 5672, "","");
        RabbitMqMessageSender messageSender = new RabbitMqMessageSender("test_remote_rabbit", "test_remote_rabbit", "test_queue", "10.64.66.114", "th2", 5672, "th2", "ahw3AeWa");
        for (int i = 0; i < 20; i++) {
            messageSender.send(Message.newBuilder()
                    .putFields("test", Value.newBuilder()
                            .setSimpleValue("value" + i)
                            .build())
                    .build());
            Thread.sleep(1000);
        }
    }
}

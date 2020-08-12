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

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.message.MessageListener;
import com.exactpro.th2.schema.message.MessageSubscriber;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;

public abstract class AbstractRabbitSubscriber<T> implements MessageSubscriber<T> {

    private static final int CLOSE_TIMEOUT = 1_000;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass() + "@" + this.hashCode());

    private final Set<MessageListener<T>> listeners = new HashSet<>();
    private final ConnectionFactory factory = new ConnectionFactory();

    private String subscriberName = null;
    private String exchangeName = null;
    private String[] queueAliases = null;

    private Connection connection;
    private Channel channel;


    public void init(@NotNull RabbitMQConfiguration configuration, @NotNull String exchangeName, String... queueTags) throws IllegalArgumentException, NullPointerException {
        if (queueTags.length < 1) {
            throw new IllegalArgumentException("Queue tags must be more than 0");
        }

        this.exchangeName = Objects.requireNonNull(exchangeName, "Exchange name in RabbitMQ can not be null");
        this.subscriberName = configuration.getSubscriberName();
        this.queueAliases = queueTags;


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
    }

    @Override
    public void start() throws Exception {
        if (queueAliases == null || exchangeName == null) {
            throw new IllegalStateException("Subscriber did not init");
        }

        if (subscriberName == null) {
            subscriberName = "rabbit_mq_subscriber";
            logger.info("Using default subscriber name: '{}'", subscriberName);
        }

        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(exchangeName, "direct");

        for (String queueTag : queueAliases) {
            DeclareOk declareResult = channel.queueDeclare(queueTag + "." + System.currentTimeMillis(), false, true, true, emptyMap());

            String queue = declareResult.getQueue();

            channel.queueBind(queue, exchangeName, queueTag);
            channel.basicConsume(queue, true, subscriberName + "." + System.currentTimeMillis(), this::handle, this::canceled);

            logger.info("Start listening exchangeName='{}', routing key='{}', queue name='{}'", exchangeName, queueTag, queue);
        }
    }

    @Override
    public boolean isClose() {
        return connection == null || !connection.isOpen();
    }

    @Override
    public void addListener(MessageListener<T> messageListener) {
        if (messageListener == null) {
            return;
        }

        synchronized (listeners) {
            listeners.add(messageListener);
        }
    }

    @Override
    public void close() throws IOException {

        synchronized (listeners) {
            listeners.forEach(MessageListener::onClose);
            listeners.clear();
        }

        if (connection != null && connection.isOpen()) {
            connection.close(CLOSE_TIMEOUT);
        }
    }

    protected abstract T valueFromBytes(byte[] body) throws Exception;

    protected abstract boolean filter(T value) throws Exception;


    private void handle(String consumeTag, Delivery delivery) {
        try {
            T value = valueFromBytes(delivery.getBody());

            if(!filter(value)){
                return;
            }

            synchronized (listeners) {
                for (MessageListener<T> listener : listeners) {
                    try {
                        listener.handler(consumeTag, value);
                    } catch (Exception listenerExc) {
                        logger.warn("Message listener from class '" + listener.getClass() + "' threw exception", listenerExc);
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Can not parse value from delivery for: " + consumeTag, e);
        }
    }

    private void canceled(String consumerTag) {
        logger.warn("Consuming cancelled for: '{}'", consumerTag);
        try {
            close();
        } catch (IOException e) {
            logger.error("Can not close subscriber with exchange name '{}' and queues '{}'", exchangeName, queueAliases);
        }
    }

}

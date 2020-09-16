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

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.mina.util.ConcurrentHashSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.message.MessageListener;
import com.exactpro.th2.schema.message.MessageSubscriber;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;


public abstract class AbstractRabbitSubscriber<T> implements MessageSubscriber<T> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass() + "@" + this.hashCode());

    private final Set<MessageListener<T>> listeners = new ConcurrentHashSet<>();

    private String exchangeName = null;
    private String subscriberName = null;
    private SubscribeTarget[] subscribeTargets = null;
    private RabbitMQConfiguration configuration = null;

    private Connection connection = null;
    private Channel channel = null;


    public void init(@NotNull RabbitMQConfiguration configuration, @NotNull String exchangeName, @NotNull SubscribeTarget... subscribeTargets) throws IllegalArgumentException, NullPointerException {
        if (subscribeTargets.length < 1) {
            throw new IllegalArgumentException("Subscribe targets must be more than 0");
        }

        this.configuration = configuration;
        this.exchangeName = Objects.requireNonNull(exchangeName, "Exchange name in RabbitMQ can not be null");
        this.subscriberName = configuration.getSubscriberName();
        this.subscribeTargets = subscribeTargets;
    }

    @Override
    public void start() throws Exception {
        if (subscribeTargets == null || exchangeName == null || configuration == null) {
            throw new IllegalStateException("Subscriber did not init");
        }

        if (subscriberName == null) {
            subscriberName = "rabbit_mq_subscriber";
            logger.info("Using default subscriber name: '{}'", subscriberName);
        }

        if (connection == null) {
            connection = ConnectionHelper.getInstance(configuration).getConnection();
        }

        if (channel == null) {
            channel = connection.createChannel();

            for (var subscribeTarget : subscribeTargets) {

                var queue = subscribeTarget.getQueue();

                var routingKey = subscribeTarget.getRoutingKey();

                channel.basicConsume(queue, true, subscriberName + "." + System.currentTimeMillis(), this::handle, this::canceled);

                logger.info("Start listening exchangeName='{}', routing key='{}', queue name='{}'", exchangeName, routingKey, queue);
            }
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

        if (channel != null) {
            if (channel.isOpen()) {
                try {
                    channel.close();
                } catch (TimeoutException e) {
                    throw new IOException("Can not close channel", e);
                }
            }

            try {
                ConnectionHelper.getInstance(configuration).close();
            } catch (TimeoutException e) {
                throw new IOException("Can not close connection", e);
            }
        }
    }

    protected abstract T valueFromBytes(byte[] body) throws Exception;

    @Nullable
    protected abstract T filter(T value) throws Exception;


    private void handle(String consumeTag, Delivery delivery) {
        try {
            T value = valueFromBytes(delivery.getBody());

            var filteredValue = filter(value);

            if (Objects.isNull(filteredValue)) {
                return;
            }

            synchronized (listeners) {
                for (MessageListener<T> listener : listeners) {
                    try {
                        listener.handler(consumeTag, filteredValue);
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
            logger.error("Can not close subscriber with exchange name '{}' and queues '{}'", exchangeName, subscribeTargets);
        }
    }

}

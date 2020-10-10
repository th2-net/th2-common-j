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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.message.MessageListener;
import com.exactpro.th2.schema.message.MessageSubscriber;
import com.exactpro.th2.schema.message.impl.rabbitmq.channel.ChannelOwner;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.exactpro.th2.schema.message.impl.rabbitmq.connection.ConnectionOwner;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;


public abstract class AbstractRabbitSubscriber<T> implements MessageSubscriber<T> {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass() + "@" + this.hashCode());

    private final Set<MessageListener<T>> listeners = new HashSet<>();

    private String exchangeName = null;
    private String subscriberName = null;
    private SubscribeTarget[] subscribeTargets = null;

    private ChannelOwner channelOwner = null;

    @Override
    public void init(@NotNull ConnectionOwner connectionOwner, @NotNull String exchangeName, String subscriberName, @NotNull SubscribeTarget... subscribeTargets) {
        if (subscribeTargets.length < 1) {
            throw new IllegalArgumentException("Subscribe targets must be more than 0");
        }

        this.channelOwner = new ChannelOwner(Objects.requireNonNull(connectionOwner, "connection cannot be null"));
        this.exchangeName = Objects.requireNonNull(exchangeName, "Exchange name in RabbitMQ can not be null");
        this.subscribeTargets = subscribeTargets;

        if (subscriberName == null) {
            this.subscriberName = "rabbit_mq_subscriber." + System.currentTimeMillis();
            logger.info("Subscriber will use default name: {}", this.subscriberName);
        } else {
            this.subscriberName = subscriberName + "." + System.currentTimeMillis();
        }
    }

    @Override
    public void start() throws Exception {
        if (channelOwner == null || subscribeTargets == null || exchangeName == null) {
            throw new IllegalStateException("Subscriber is not initialized");
        }

        channelOwner.tryToCreateChannel();
        channelOwner.runOnChannel(channel -> {
            for (SubscribeTarget subscribeTarget : subscribeTargets) {

                var queue = subscribeTarget.getQueue();

                var routingKey = subscribeTarget.getRoutingKey();

                if (isOpen()) {
                    channel.basicConsume(queue, true, subscriberName, this::handle, this::canceled);
                }

                logger.info("Start listening exchangeName='{}', routing key='{}', queue name='{}'", exchangeName, routingKey, queue);
            }
            return null;
        }, () -> {
            throw new IOException("Can not start listening. Channel is close");
        });
    }

    @Override
    public boolean isOpen() {
        return channelOwner.isOpen();
    }

    @Override
    public void addListener(MessageListener<T> messageListener) {
        if (messageListener == null) {
            return;
        }

        listeners.add(messageListener);
    }

    @Override
    public void close() throws Exception {
        listeners.forEach(MessageListener::onClose);
        listeners.clear();

        channelOwner.close();
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

            for (MessageListener<T> listener : listeners) {
                try {
                    listener.handler(consumeTag, filteredValue);
                } catch (Exception listenerExc) {
                    logger.warn("Message listener from class '{}' threw exception", listener.getClass(), listenerExc);
                }
            }
        } catch (Exception e) {
            logger.error("Can not parse value from delivery for: {}", consumeTag, e);
        }
    }

    private void canceled(String consumerTag) {
        logger.warn("Consuming cancelled for: '{}'", consumerTag);
        try {
            close();
        } catch (Exception e) {
            logger.error("Can not close subscriber with exchange name '{}' and queues '{}'", exchangeName, subscribeTargets, e);
        }
    }

}

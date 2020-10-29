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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.message.MessageListener;
import com.exactpro.th2.schema.message.MessageSubscriber;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.exactpro.th2.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.rabbitmq.client.Delivery;


public abstract class AbstractRabbitSubscriber<T> implements MessageSubscriber<T> {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass() + "@" + this.hashCode());

    private final List<MessageListener<T>> listeners = new CopyOnWriteArrayList<>();
    private final AtomicReference<String> exchangeName = new AtomicReference<>();
    private final AtomicReference<SubscribeTarget> subscribeTarget = new AtomicReference<>();
    private final AtomicReference<ConnectionManager> connectionManager = new AtomicReference<>();
    private final AtomicReference<String> consumerTag = new AtomicReference<>();

    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull SubscribeTarget subscribeTarget) {
        Objects.requireNonNull(connectionManager, "Connection cannot be null");
        Objects.requireNonNull(subscribeTarget, "Subscriber target is can not be null");
        Objects.requireNonNull(exchangeName, "Exchange name in RabbitMQ can not be null");


        if (this.connectionManager.get() != null || this.exchangeName.get() != null || this.subscribeTarget.get() != null) {
            throw new IllegalStateException("Subscriber is already initialize");
        }

        this.connectionManager.set(connectionManager);
        this.subscribeTarget.set(subscribeTarget);
        this.exchangeName.set(exchangeName);
    }

    @Override
    public void start() throws Exception {
        ConnectionManager connectionManager = this.connectionManager.get();
        SubscribeTarget target = subscribeTarget.get();
        String exchange = exchangeName.get();

        if (connectionManager == null || target == null || exchange == null) {
            throw new IllegalStateException("Subscriber is not initialized");
        }

        try {
            var queue = target.getQueue();
            var routingKey = target.getRoutingKey();

            consumerTag.updateAndGet(tag -> {
                if (tag != null) {
                    throw new IllegalStateException("Subscriber already started");
                }

                try {
                    tag = connectionManager.basicConsume(queue, this::handle, this::canceled);
                    logger.info("Start listening consumerTag='{}',exchangeName='{}', routing key='{}', queue name='{}'", tag, exchangeName, routingKey, queue);
                } catch (IOException e) {
                    throw new IllegalStateException("Can not start subscribe to queue = " + queue, e);
                }

                return tag;
            });
        } catch (Exception e) {
            throw new IOException("Can not start listening", e);
        }
    }

    @Override
    public void addListener(MessageListener<T> messageListener) {
        listeners.add(messageListener);
    }

    @Override
    public boolean isOpen() {
        ConnectionManager connectionManager = this.connectionManager.get();
        return consumerTag.get() != null && connectionManager != null && connectionManager.isOpen();
    }

    @Override
    public void close() throws Exception {
        ConnectionManager connectionManager = this.connectionManager.get();
        if (connectionManager == null) {
            throw new IllegalStateException("Subscriber is not initialized");
        }

        String tag = consumerTag.getAndSet(null);
        if (tag == null) {
            return;
        }

        connectionManager.basicCancel(tag);

        listeners.forEach(MessageListener::onClose);
        listeners.clear();
    }

    protected abstract T valueFromBytes(byte[] body) throws Exception;

    protected abstract String toShortDebugString(T value);

    @Nullable
    protected abstract T filter(T value) throws Exception;


    private void handle(String consumeTag, Delivery delivery) {
        try {
            T value = valueFromBytes(delivery.getBody());

            if (logger.isDebugEnabled()) {
                logger.debug("The received message {}", toShortDebugString(value));
            }

            var filteredValue = filter(value);

            if (Objects.isNull(filteredValue)) {
                logger.debug("Message is filtred");
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
            logger.error("Can not close subscriber with exchange name '{}' and queues '{}'", exchangeName.get(), subscribeTarget.get(), e);
        }
    }

}

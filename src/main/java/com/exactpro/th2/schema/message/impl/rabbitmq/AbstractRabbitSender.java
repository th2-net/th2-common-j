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
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.message.MessageSender;
import com.exactpro.th2.schema.message.impl.rabbitmq.connection.ConnectionOwner;

public abstract class AbstractRabbitSender<T> implements MessageSender<T> {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final AtomicReference<String> sendQueue = new AtomicReference<>();
    private final AtomicReference<String> exchangeName = new AtomicReference<>();
    private final AtomicReference<ConnectionOwner> connectionOwner = new AtomicReference<>();

    @Override
    public void init(@NotNull ConnectionOwner connectionOwner, @NotNull String exchangeName, @NotNull String sendQueue) {
        Objects.requireNonNull(connectionOwner, "Connection can not be null");
        Objects.requireNonNull(exchangeName, "Exchange name can not be null");
        Objects.requireNonNull(sendQueue, "Send queue can not be null");

        if (this.connectionOwner.get() != null && this.sendQueue.get() != null && this.exchangeName.get() != null) {
            throw new IllegalStateException("Sender is already initialize");
        }

        this.connectionOwner.updateAndGet(connection -> {
            if (connection == null) {
                connection = connectionOwner;
            }
            return connection;
        });

        this.exchangeName.updateAndGet(exchange -> {
            if (exchange == null) {
                exchange = exchangeName;
            }
            return exchange;
        });

        this.sendQueue.updateAndGet(queue -> {
            if (queue == null) {
                queue = sendQueue;
            }
            return queue;
        });
    }

    @Override
    public boolean isOpen() {
        ConnectionOwner connectionOwner = this.connectionOwner.get();
        return connectionOwner != null && connectionOwner.isOpen();
    }

    @Override
    public void send(T value) throws IOException {

        try {
            ConnectionOwner connection = this.connectionOwner.get();
            connection.basicPublish(exchangeName.get(), sendQueue.get(), null, valueToBytes(value));

            if (logger.isDebugEnabled()) {
                logger.debug("Message sent to exchangeName='{}', routing key='{}': '{}'",
                        exchangeName, sendQueue, toShortDebugString(value));
            }
        } catch (Exception e) {
            throw new IOException("Can not send message: " + toShortDebugString(value), e);
        }
    }

    protected String toShortDebugString(T value) {
        return value.toString();
    }

    protected abstract byte[] valueToBytes(T value);


}

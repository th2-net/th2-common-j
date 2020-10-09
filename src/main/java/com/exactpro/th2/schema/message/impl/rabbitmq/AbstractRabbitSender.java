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
import java.util.concurrent.TimeoutException;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.message.MessageSender;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public abstract class AbstractRabbitSender<T> implements MessageSender<T> {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private Connection connection = null;
    private Channel channel = null;

    private String sendQueue = null;
    private String exchangeName = null;

    @Override
    public void init(@NotNull Connection connection, @NotNull String exchangeName, @NotNull String sendQueue) {
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");
        this.exchangeName = Objects.requireNonNull(exchangeName, "Exchange name can not be null");
        this.sendQueue = sendQueue;
    }

    @Override
    public void start() throws Exception {
        if (connection == null || sendQueue == null || exchangeName == null) {
            throw new IllegalStateException("Sender is not initialized");
        }

        if (channel == null) {
            channel = connection.createChannel();
        }
    }

    @Override
    public boolean isOpen() {
        return channel != null && channel.isOpen();
    }

    @Override
    public void send(T value) throws IOException {
        if (channel == null) {
            throw new IllegalStateException("Can not send. Sender was not started");
        }

        channel.basicPublish(exchangeName, sendQueue, null, valueToBytes(value));

        if (logger.isDebugEnabled()) {
            logger.debug("Message sent to exchangeName='{}', routing key='{}': '{}'",
                    exchangeName, sendQueue, toShortDebugString(value));
        }
    }

    @Override
    public void close() throws IOException, TimeoutException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    protected String toShortDebugString(T value) {
        return value.toString();
    }

    protected abstract byte[] valueToBytes(T value);
}

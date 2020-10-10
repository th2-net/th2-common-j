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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.schema.message.MessageSender;
import com.exactpro.th2.schema.message.impl.rabbitmq.channel.ChannelOwner;
import com.exactpro.th2.schema.message.impl.rabbitmq.connection.ConnectionOwner;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public abstract class AbstractRabbitSender<T> implements MessageSender<T> {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private String sendQueue = null;
    private String exchangeName = null;
    private ChannelOwner channelOwner = null;

    @Override
    public void init(@NotNull ConnectionOwner connectionOwner, @NotNull String exchangeName, @NotNull String sendQueue) {
        this.channelOwner = new ChannelOwner(Objects.requireNonNull(connectionOwner, "connection cannot be null"));
        this.exchangeName = Objects.requireNonNull(exchangeName, "Exchange name can not be null");
        this.sendQueue = sendQueue;
    }

    @Override
    public void start() throws Exception {
        if (channelOwner == null || sendQueue == null || exchangeName == null) {
            throw new IllegalStateException("Sender is not initialized");
        }

        try {
            channelOwner.tryToCreateChannel();
        } catch (IllegalStateException e) {
            throw new IllegalStateException("Can not start closed sender", e);
        }
    }

    @Override
    public boolean isOpen() {
        return channelOwner.isOpen();
    }

    @Override
    public void send(T value) throws IOException {
        if (channelOwner.wasClosed()) {
            throw new IllegalStateException("Can not send message '" + toShortDebugString(value) + "'. Sender was closed");
        }

        try {
            channelOwner.runOnChannel(channel -> {
                channel.basicPublish(exchangeName, sendQueue, null, valueToBytes(value));
                if (logger.isDebugEnabled()) {
                    logger.debug("Message sent to exchangeName='{}', routing key='{}': '{}'",
                            exchangeName, sendQueue, toShortDebugString(value));
                }
                return null;
            }, () -> {
                throw new Exception("Can not send message, because sender closed.");
            });
        } catch (Exception e) {
            throw new IOException("Can not send message: " + toShortDebugString(value), e);
        }
    }

    @Override
    public void close() throws Exception {

    }

    protected String toShortDebugString(T value) {
        return value.toString();
    }

    protected abstract byte[] valueToBytes(T value);


}

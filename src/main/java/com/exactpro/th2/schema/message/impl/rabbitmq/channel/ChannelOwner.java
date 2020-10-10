/*
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
 */
package com.exactpro.th2.schema.message.impl.rabbitmq.channel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.schema.message.impl.rabbitmq.connection.ConnectionOwner;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import lombok.SneakyThrows;

public class ChannelOwner implements AutoCloseable{

    private final ConnectionOwner connectionOwner;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final ThreadLocal<Channel> channel = new ThreadLocal<>();
    private final List<Channel> resources = new CopyOnWriteArrayList<>();

    public ChannelOwner(@NotNull ConnectionOwner connectionOwner) {
        this.connectionOwner = connectionOwner;
    }

    public void tryToCreateChannel() throws IOException, IllegalStateException {
        if (isClosed.get()) {
            throw new IllegalStateException("Can not create new channel. Channel owner already closed");
        }

        if (channel.get() == null) {
            channel.set(connectionOwner.createChannel());
            resources.add(channel.get());
        }
    }

    @SneakyThrows
    public boolean isOpen() {
        return runOnChannel(channel -> channel.isOpen(), () -> false);
    }

    public boolean wasClosed() {
        return isClosed.get();
    }

    @Override
    public void close() throws Exception {
        if (isClosed.get()) {
            return;
        }

        isClosed.set(true);
        Collection<Exception> exceptions = new ArrayList<>();

        for (AutoCloseable resource : resources) {
            try {
                synchronized (resource) {
                    resource.close();
                }
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        if (!exceptions.isEmpty()) {
            Exception exception = new Exception("Can not close message sender");
            exceptions.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    public <T> T runOnChannel(@NotNull ChannelUsage<T> action, OnChannelClosed<T> onClose) throws Exception {
        if (isClosed.get()) {
            channel.remove();
        }

        Channel tmp = channel.get();
        if (tmp != null) {

            boolean channelIsClose;
            do {
                synchronized (tmp) {
                    channelIsClose = !tmp.isOpen();
                }
                if (channelIsClose) {
                    Thread.yield();
                }
            } while (channelIsClose && !isClosed.get());

            if (!isClosed.get()) {
                synchronized (tmp) {
                    return action.use(tmp);
                }
            }

            if (onClose != null) {
                return onClose.onClosed();
            }
        } else if (onClose != null) {
            return onClose.onClosed();
        }

        return null;
    }
}

/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.ShutdownNotifier;

class ChannelHolder implements ConfirmListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelHolder.class);
    private static final Consumer<Map<Long, PublicationHolder>> DO_NOTHING = it -> {};
    private final Lock lock = new ReentrantLock();
    private final Supplier<Channel> supplier;
    private final BiConsumer<ShutdownNotifier, Boolean> reconnectionChecker;
    private final int maxCount;
    private final boolean retryOnPublishNotConfirmed;

    private final NavigableMap<Long, PublicationHolder> publishedData;

    @GuardedBy("lock")
    private int pending;
    @GuardedBy("lock")
    private Future<?> check;
    @GuardedBy("lock")
    private Channel channel;

    public ChannelHolder(
            Supplier<Channel> supplier,
            BiConsumer<ShutdownNotifier, Boolean> reconnectionChecker,
            int maxCount,
            boolean retryOnPublishNotConfirmed
    ) {
        this.supplier = Objects.requireNonNull(supplier, "'Supplier' parameter");
        this.reconnectionChecker = Objects.requireNonNull(reconnectionChecker, "'Reconnection checker' parameter");
        this.maxCount = maxCount;
        this.retryOnPublishNotConfirmed = retryOnPublishNotConfirmed;
        publishedData = retryOnPublishNotConfirmed ? new ConcurrentSkipListMap<>() : Collections.emptyNavigableMap();
    }

    public void withLock(ChannelConsumer consumer) throws IOException {
        withLock(true, consumer);
    }

    public void publish(String exchange, String routingKey, BasicProperties props, byte[] body) throws IOException {
        withLock(channel -> {
            if (retryOnPublishNotConfirmed) {
                publishedData.put(channel.getNextPublishSeqNo(),
                        new PublicationHolder(exchange, routingKey, props, body));
            }
            channel.basicPublish(exchange, routingKey, props, body);
        });
    }

    private void publish(PublicationHolder holder) throws IOException {
        publish(holder.getExchange(), holder.getRoutingKey(), holder.getProps(), holder.getBody());
    }

    public void withLock(Runnable action) {
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    public void withLock(boolean waitForRecovery, ChannelConsumer consumer) throws IOException {
        lock.lock();
        try {
            consumer.consume(getChannel(waitForRecovery));
        } finally {
            lock.unlock();
        }
    }

    public <T> T mapWithLock(ChannelMapper<T> mapper) throws IOException {
        lock.lock();
        try {
            return mapper.map(getChannel());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Decreases the number of unacked messages.
     * If the number of unacked messages is less than {@link #maxCount}
     * the <b>onWaterMarkDecreased</b> action will be called.
     * The future created in {@link #acquireAndSubmitCheck(Supplier)} method will be canceled
     *
     * @param onWaterMarkDecreased the action that will be executed when the number of unacked messages is less than {@link #maxCount} and there is a future to cancel
     */
    public void release(Runnable onWaterMarkDecreased) {
        lock.lock();
        try {
            pending--;
            if (pending < maxCount && check != null) {
                check.cancel(true);
                check = null;
                onWaterMarkDecreased.run();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Increases the number of unacked messages.
     * If the number of unacked messages is higher than or equal to {@link #maxCount}
     * the <b>futureSupplier</b> will be invoked to create a task
     * that either will be executed or canceled when number of unacked message will be less that {@link #maxCount}
     *
     * @param futureSupplier creates a future to track the task that should be executed until the number of unacked message is not less than {@link #maxCount}
     */
    public void acquireAndSubmitCheck(Supplier<Future<?>> futureSupplier) {
        lock.lock();
        try {
            pending++;
            if (reachedPendingLimit() && check == null) {
                check = futureSupplier.get();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean reachedPendingLimit() {
        lock.lock();
        try {
            return pending >= maxCount;
        } finally {
            lock.unlock();
        }
    }

    private Channel getChannel() throws IOException {
        return getChannel(true);
    }

    private Channel getChannel(boolean waitForRecovery) throws IOException {
        if (channel == null) {
            channel = supplier.get();
            channel.confirmSelect();
            channel.addConfirmListener(this);
        }
        reconnectionChecker.accept(channel, waitForRecovery);
        return channel;
    }

    @Override
    public void handleAck(long deliveryTag, boolean multiple) throws IOException {
        LOGGER.trace("Ack for {} multiple: {} received", deliveryTag, multiple);
        if (!retryOnPublishNotConfirmed) {
            return;
        }
        if (multiple) {
            notifiedPublications(deliveryTag, "ack", DO_NOTHING);
        } else {
            removeNotifiedPublication(deliveryTag, "ack");
        }
    }

    @Override
    public void handleNack(long deliveryTag, boolean multiple) throws IOException {
        LOGGER.warn("Nack for {} multiple: {} received", deliveryTag, multiple);
        if (!retryOnPublishNotConfirmed) {
            return;
        }
        if (multiple) {
            notifiedPublications(deliveryTag, "nack", notifiedData -> {
                for (PublicationHolder value : notifiedData.values()) {
                    republish(value);
                }
            });
        } else {
            PublicationHolder holder = removeNotifiedPublication(deliveryTag, "nack");
            republish(holder);
        }
    }

    private void republish(PublicationHolder value) {
        if (value == null) {
            return;
        }
        try {
            publish(value);
        } catch (IOException ex) {
            LOGGER.error("Cannot republish data {}", value, ex);
        }
    }

    private void notifiedPublications(long deliveryTag, String type, Consumer<Map<Long, PublicationHolder>> action) {
        NavigableMap<Long, PublicationHolder> headMap = publishedData.headMap(deliveryTag, true/*include key*/);
        if (headMap.isEmpty()) {
            LOGGER.warn("Received {} for unknown delivery {}", type, deliveryTag);
        }
        try {
            action.accept(headMap);
        } finally {
            headMap.clear();
        }
    }

    @Nullable
    private PublicationHolder removeNotifiedPublication(long deliveryTag, String type) {
        PublicationHolder holder = publishedData.remove(deliveryTag);
        if (holder == null) {
            LOGGER.warn("Received {} for unknown delivery {}", type, deliveryTag);
        }
        return holder;
    }
}

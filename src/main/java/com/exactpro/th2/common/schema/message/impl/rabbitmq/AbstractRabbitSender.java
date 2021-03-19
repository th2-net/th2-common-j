/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq;

import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ResendMessageConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.retry.CreateChannelException;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.retry.RetryBuilder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import io.prometheus.client.Counter;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractRabbitSender<T> implements MessageSender<T>, ConfirmListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSender.class);

    private final ConnectionManager connectionManager;
    private final ResendMessageConfiguration resendMessageConfiguration;
    private final String routingKey;
    private final String exchangeName;

    private final ManualResetEvent semaphore = new ManualResetEvent(true);
    private final AtomicInteger countAttempts = new AtomicInteger(0);

    private final Object channelLock = new Object();
    private Channel channel;

    private final Set<Long> sendData = ConcurrentHashMap.newKeySet();

    private final ConcurrentMap<Long, T> sendedData = new ConcurrentHashMap<>();
    private final Set<Long> resended = ConcurrentHashMap.newKeySet();

    private long minSeq = -1L;

    private final HealthMetrics metrics = new HealthMetrics(this);

    protected AbstractRabbitSender(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull String routingKey) {
        this.connectionManager = Objects.requireNonNull(connectionManager, "Connection manager can not be null");
        this.exchangeName = Objects.requireNonNull(exchangeName, "Exchange name can not be null");
        this.routingKey = Objects.requireNonNull(routingKey, "Send queue can not be null");
        this.resendMessageConfiguration = connectionManager.getConfiguration().getResendMessageConfiguration();

        createChannel();
        LOGGER.info("{}:{} initialised with queue {}", getClass().getSimpleName(), hashCode(), routingKey);
    }

    private void acquireSemaphore() throws InterruptedException {
        while (!semaphore.waitOne(1, TimeUnit.SECONDS)) {
            LOGGER.warn("Send operation is blocked, exchangeName='{}', routing key ='{}'", exchangeName, routingKey);
        }
    }

    @Override
    public void send(T value) {
        Objects.requireNonNull(value, "Value for send can not be null");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Try to send message to exchangeName='{}', routing key='{}': '{}'",
                    exchangeName, routingKey, toShortDebugString(value));
        }

        try {
            acquireSemaphore();
            sendSync(value);
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Can not send message to exchangeName='{}', routing key ='{}': '{}'", exchangeName, routingKey, toShortDebugString(value), e);
            }
        }
    }

    @Override
    public void handleAck(long deliveryTag, boolean multiple) {

        countAttempts.set(0);

        if (multiple) {
            if (minSeq < 0) {
                minSeq = deliveryTag;
            }

            for (; minSeq <= deliveryTag; minSeq++) {
                T message = sendedData.remove(minSeq);
                resended.remove(minSeq);

                if (LOGGER.isDebugEnabled() && message != null) {
                    LOGGER.debug("Message sent to exchangeName='{}', routing key='{}': '{}'",
                            exchangeName, routingKey, toShortDebugString(message));
                }
            }

            lock.lock();
            notLock.signalAll();
            lock.unlock();

        } else {
            if (minSeq < 0 || minSeq + 1 == deliveryTag) {
                minSeq = deliveryTag;
            }

            T message = sendedData.remove(deliveryTag);
            resended.remove(deliveryTag);

            if (LOGGER.isDebugEnabled() && message != null) {
                LOGGER.debug("Message sent to exchangeName='{}', routing key='{}': '{}'",
                        exchangeName, routingKey, toShortDebugString(message));
            }

            lock.lock();
            notLock.signalAll();
            lock.unlock();
        }
    }

    @Override
    public void handleNack(long deliveryTag, boolean multiple) {
        if (multiple) {
            if (minSeq < 0) {
                minSeq = deliveryTag;
            }

            for (; minSeq <= deliveryTag; minSeq++) {
                LOGGER.trace("Message with delivery tag '{}' is rejected", minSeq);
                T value = sendedData.remove(minSeq);
                if (value != null) {
                    final long currentSeq = minSeq;
                    resended.add(currentSeq);
                    resend(value).thenRun(() -> resended.remove(currentSeq));
                }

            }
        } else {
            if (minSeq < 0 || minSeq + 1 == deliveryTag) {
                minSeq = deliveryTag;
            }

            LOGGER.trace("Message with delivery tag '{}' is rejected", deliveryTag);
            T value = sendedData.remove(deliveryTag);
            if (value != null) {
                resended.add(deliveryTag);
                resend(value).thenRun(() -> resended.remove(deliveryTag));
            }
            resended.remove(deliveryTag);
        }
    }

    protected abstract Counter getDeliveryCounter();

    protected abstract Counter getContentCounter();

    protected abstract int extractCountFrom(T message);

    protected abstract String toShortDebugString(T value);

    protected abstract byte[] valueToBytes(T value);

    private void createChannel() {
        try {
            canSend.set(false);

            synchronized (channelLock) {
                channel = Objects.requireNonNull(connectionManager.createChannel(), "Channel can not be null");
                channel.confirmSelect();
                channel.addConfirmListener(this);
            }

            Map<Long, T> dataForSend = new HashMap<>(sendedData);
            sendedData.clear();
            resended.clear(); // Sender will be blocked, because canSend if false

            for (T value : dataForSend.values()) {
                sendSync(value);
            }

            canSend.set(true);
            lock.lock();
            notLock.signalAll();
            lock.unlock();
        } catch (Exception e) {
            throw new IllegalStateException("Can not create channel", e);
        }
    }

    private void sendSync(T value) throws ExecutionException, InterruptedException {
        addExecutedHandler(createSendRetryBuilder(value,false), value).get();
    }

    private CompletableFuture<?> resend(T value) {
        LOGGER.warn("Retry send message to exchangeName='{}', routing key='{}': '{}'",
                exchangeName, routingKey, toShortDebugString(value));

        RetryBuilder<Void> builder = connectionManager.createRetryBuilder();

        long delay = getNextDelay();
        if (delay > 0) {
            builder.setDelay(delay, TimeUnit.MILLISECONDS);
        }

        return addExecutedHandler(createSendRetryBuilder(builder, value, true), value);
    }

    private CompletableFuture<Void> createSendRetryBuilder(RetryBuilder<Void> builder, T value, boolean addToResended) {
        byte[] bytes = valueToBytes(value);

        return builder.setMetrics(metrics)
                .build(ignore -> {
                    long seq;
                    synchronized (channelLock) {
                        seq = channel.getNextPublishSeqNo();
                        sendedData.put(seq, value);
                        channel.basicPublish(exchangeName, routingKey, null, bytes);
                    }

                    if (addToResended) {
                        resended.add(seq);
                    }
                    return null;
                });
    }

    private CompletableFuture<Void> createSendRetryBuilder(T value, boolean addToResended) {
        return createSendRetryBuilder(connectionManager.createRetryBuilder(), value, addToResended);
    }

    private CompletableFuture<?> addExecutedHandler(CompletableFuture<?> future, T value) {
        return future.exceptionally(ex -> {
            if (ex instanceof CreateChannelException) {
                createChannel(); //FIXME: the createChannel method resends all data in sync mode
            } else if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Can not send message to exchangeName='{}', routing key ='{}': '{}'", exchangeName, routingKey, toShortDebugString(value), ex);
            }
            return null;
        });
    }

    private long getNextDelay() {
        long count = countAttempts.incrementAndGet();
        long delay = (long) (Math.E * count * resendMessageConfiguration.getMinDelay());
        return resendMessageConfiguration.getMaxDelay() > 0 ? Math.min(delay, resendMessageConfiguration.getMaxDelay()) :  delay;
    }

    private static final AtomicLong SEND_DATA_COUNTER = new AtomicLong(0);

    private static class SendData<T> {
        private final long id = SEND_DATA_COUNTER.incrementAndGet();
        private final T data;

        private SendData(T data) {
            this.data = data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SendData<?> sendData = (SendData<?>)o;

            return new EqualsBuilder().append(id, sendData.id).isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(id).toHashCode();
        }
    }

    private static class ManualResetEvent {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private volatile boolean isOpen;

        public ManualResetEvent(boolean open) {
            isOpen = open;
        }

        public boolean waitOne(long time, TimeUnit unit) throws InterruptedException {
            if (!isOpen) {
                try {
                    lock.lock();
                    if (!isOpen) {
                        //noinspection ResultOfMethodCallIgnored
                        condition.await(time, unit);
                    }
                } finally {
                    lock.unlock();
                }
            }

            return isOpen;
        }

        public void set() {
            if (!isOpen) {
                try {
                    lock.lock();
                    if (!isOpen) {
                        isOpen = true;
                        condition.signalAll();
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        public void reset() {
            isOpen = false;
        }
    }
}

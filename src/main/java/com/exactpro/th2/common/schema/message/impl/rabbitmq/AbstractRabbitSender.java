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

import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ResendMessageConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.retry.CreateChannelException;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.retry.RetryBuilder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import io.prometheus.client.Counter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractRabbitSender<T> implements MessageSender<T>, ConfirmListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSender.class);

    private final ConnectionManager connectionManager;
    private final ResendMessageConfiguration resendMessageConfiguration;
    private final String sendQueue;
    private final String exchangeName;


    private final Lock lock = new ReentrantLock();
    private final Condition notLock = lock.newCondition();
    private final AtomicBoolean canSend = new AtomicBoolean(true);
    private final AtomicInteger countAttempts = new AtomicInteger(0);


    private final Object channelLock = new Object();
    private Channel channel;

    private final Map<Long, T> sendedData = new ConcurrentHashMap<>();
    private final Set<Long> resended = ConcurrentHashMap.newKeySet();

    private long minSeq = -1L;

    private final HealthMetrics metrics = new HealthMetrics(
            CommonMetrics.registerLiveness(this),
            CommonMetrics.registerReadiness(this)
    );

    protected AbstractRabbitSender(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull String sendQueue) {
        this.connectionManager = Objects.requireNonNull(connectionManager, "Connection manager can not be null");
        this.exchangeName = Objects.requireNonNull(exchangeName, "Exchange name can not be null");
        this.sendQueue = Objects.requireNonNull(sendQueue, "Send queue can not be null");
        this.resendMessageConfiguration = connectionManager.getConfiguration().getResendMessageConfiguration();

        createChannel();
        LOGGER.info("{}:{} initialised with queue {}", getClass().getSimpleName(), hashCode(), sendQueue);
    }

    @Override
    public void send(T value) {
        Objects.requireNonNull(value, "Value for send can not be null");

        while (canSend.get() && !resended.isEmpty()) {
            try {
                lock.lock();
                notLock.await();
                lock.unlock();
            } catch (InterruptedException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Can not send message to exchangeName='{}', routing key ='{}': '{}'", exchangeName, sendQueue, toShortDebugString(value), e);
                }
                Thread.currentThread().interrupt();
            }
        }

        if (!sendSync(value)) {
            return;
        }

        Counter counter = getDeliveryCounter();
        counter.inc();
        Counter contentCounter = getContentCounter();
        contentCounter.inc(extractCountFrom(value));

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Message try to send to exchangeName='{}', routing key='{}': '{}'",
                    exchangeName, sendQueue, toShortDebugString(value));
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
                            exchangeName, sendQueue, toShortDebugString(message));
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
                        exchangeName, sendQueue, toShortDebugString(message));
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
                    resend(value);
                }
                resended.remove(minSeq);
            }
        } else {
            if (minSeq < 0 || minSeq + 1 == deliveryTag) {
                minSeq = deliveryTag;
            }

            LOGGER.trace("Message with delivery tag '{}' is rejected", deliveryTag);
            T value = sendedData.remove(deliveryTag);
            if (value != null) {
                resend(value);
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

            HashMap<Long, T> dataForSend = new HashMap<>(sendedData);
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

    private boolean sendSync(T value) {
        try {
            addExecutedHandler(createSendRetryBuilder(value,false).createWithActionNow(), value)
                    .get();
            return true;
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Can not send message to exchangeName='{}', routing key ='{}': '{}'", exchangeName, sendQueue, toShortDebugString(value), e);
            return false;
        }
    }

    private void resend(T value) {
        LOGGER.warn("Retry send message to exchangeName='{}', routing key='{}': '{}'",
                exchangeName, sendQueue, toShortDebugString(value));

        RetryBuilder<Void> builder = createSendRetryBuilder(value, true);

        long delay = getNextDelay();
        if (delay > 0) {
            builder.setDelay(delay, TimeUnit.MILLISECONDS);
        }

        builder.createWithAction().thenAccept(f -> addExecutedHandler(f, value));
    }

    private RetryBuilder<Void> createSendRetryBuilder(T value, boolean addToResended) {
        byte[] bytes = valueToBytes(value);

        return connectionManager.<Void>createRetryBuilder()
                .setChannelCreator(() -> null)
                .setChannel(null)
                .setMetrics(metrics)
                .setAction(ignore -> {
                    long seq;
                    synchronized (channelLock) {
                        seq = channel.getNextPublishSeqNo();
                        sendedData.put(seq, value);
                        channel.basicPublish(exchangeName, sendQueue, null, bytes);
                    }

                    if (addToResended) {
                        resended.add(seq);
                    }
                });
    }

    private CompletableFuture<?> addExecutedHandler(CompletableFuture<?> future, T value) {
        return future.exceptionally(ex -> {
            if (ex instanceof CreateChannelException) {
                createChannel();
            } else if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Can not send message to exchangeName='{}', routing key ='{}': '{}'", exchangeName, sendQueue, toShortDebugString(value), ex);
            }
            return null;
        });
    }

    private long getNextDelay() {
        long count = countAttempts.incrementAndGet();
        long delay = (long) (Math.E * count * resendMessageConfiguration.getMinDelay());
        return resendMessageConfiguration.getMaxDelay() > 0 ? Math.min(delay, resendMessageConfiguration.getMaxDelay()) :  delay;
    }
}

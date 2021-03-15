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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.metrics.MetricMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;

public class Resender implements ConfirmListener, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Resender.class);

    private Channel channel;
    private final Supplier<Channel> channelSupplier;
    private final Object channelLock = new Object();
    private final RabbitMQConfiguration configuration;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean connectionChecker;
    private final ExecutorService tasker;
    private final Runnable unlockSender;

    private final MetricMonitor livenessMonitor;
    private final MetricMonitor readinessMonitor;

    private final Object dataStatusLock = new Object();
    private final Map<Long, SendData> sendedData = new HashMap<>();
    private final Map<Long, Boolean> dataStatus = new HashMap<>();
    private long minSeq = -1L;

    public Resender(@NotNull Supplier<Channel> channelCreator,
                    Runnable unlockSenderFunction,
                    @NotNull ExecutorService tasker,
                    @NotNull ScheduledExecutorService scheduler,
                    @NotNull AtomicBoolean connectionChecker,
                    @NotNull RabbitMQConfiguration configuration,
                    @NotNull MetricMonitor livenessMonitor,
                    @NotNull MetricMonitor readinessMonitor) {
        this.unlockSender = unlockSenderFunction;
        this.channelSupplier = Objects.requireNonNull(channelCreator, "Channel creator can not be null");
        createChannel();
        this.tasker = Objects.requireNonNull(tasker, "Tasker can not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "Scheduler can not be null");
        this.connectionChecker = Objects.requireNonNull(connectionChecker, "Connection checker can not be null");
        this.configuration = Objects.requireNonNull(configuration, "Configuration can not be null");
        this.livenessMonitor = Objects.requireNonNull(livenessMonitor, "Liveness monitor can not be null");
        this.readinessMonitor = Objects.requireNonNull(readinessMonitor, "Readiness monitor can not be null");
    }

    private void createChannel() {
        try {
            synchronized (channelLock) {
                channel = Objects.requireNonNull(channelSupplier.get(), "Channel can not be null");
                channel.confirmSelect();
                channel.addConfirmListener(this);
            }

            synchronized (dataStatusLock) {
                sendedData.clear();
                dataStatus.clear();
            }

            if (unlockSender != null) {
                unlockSender.run();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Can not create channel", e);
        }
    }

    public void sendAsync(SendData data) {
        RetryRequest<Long> retryRequest = new RetryRequest<>(channel, connectionChecker, configuration, scheduler, tasker, livenessMonitor, readinessMonitor) {
            @Override
            protected Long action(Channel channel) throws IOException, AlreadyClosedException {
                long seq;
                synchronized (channelLock) {
                    seq = channel.getNextPublishSeqNo();
                    channel.basicPublish(data.getExchange(), data.getRoutingKey(), data.getProps(), data.getBody());
                }
                return seq;
            }
        };

        tasker.execute(retryRequest);
        retryRequest.getCompletableFuture().exceptionally(ex -> {
            if (ex instanceof CreateChannelException) {
                createChannel();
            } else {
                LOGGER.warn("Can not send message to routing key '{}'", data.getRoutingKey(), ex);
            }
            return null;
        }).thenAcceptAsync(seq -> {
            if (seq == null) {
                sendAsync(data);
            } else {
                Boolean status;
                synchronized (dataStatusLock) {
                    status = dataStatus.remove(seq);
                    if (status == null) {
                        sendedData.put(seq, data);
                    }
                }

                if (status != null) {
                    if (status) {
                        success(data);
                    } else {
                        resend(data);
                    }
                }
            }
        }, tasker);
    }

    @Override
    public void handleAck(long deliveryTag, boolean multiple) {
        if (multiple) {
            if (minSeq < 0) {
                minSeq = deliveryTag;
            }

            for (; minSeq <= deliveryTag; minSeq++) {
                SendData data;
                synchronized (dataStatusLock) {
                    data = sendedData.remove(minSeq);
                    if (data == null) {
                        dataStatus.put(minSeq, true);
                    }
                }
                success(data);
            }

        } else {
            if (minSeq < 0 || minSeq + 1 == deliveryTag) {
                minSeq = deliveryTag;
            }

            SendData data;
            synchronized (dataStatusLock) {
                data = sendedData.remove(minSeq);
                if (data == null) {
                    dataStatus.put(minSeq, true);
                }
            }
            success(data);
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
                SendData data;
                synchronized (dataStatusLock) {
                    data = sendedData.remove(minSeq);
                    if (data == null) {
                        dataStatus.put(minSeq, false);
                    }
                }
                resend(data);
            }
        } else {
            if (minSeq < 0 || minSeq + 1 == deliveryTag) {
                minSeq = deliveryTag;
            }

            LOGGER.trace("Message with delivery tag '{}' is rejected", deliveryTag);
            SendData data;
            synchronized (dataStatusLock) {
                data = sendedData.remove(minSeq);
                if (data == null) {
                    dataStatus.put(minSeq, false);
                }
            }
            resend(data);
        }
    }

    private void resend(SendData data) {
        if (data != null) {
            data.resend(scheduler, configuration.getResendMessageConfiguration(), this);
        }
    }

    private void success(SendData data) {
        if (data != null) {
            data.success(configuration.getResendMessageConfiguration());
        }
    }

    @Override
    public void close() throws IOException {
        try {
            channel.close();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}

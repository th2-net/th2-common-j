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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.retry;

import com.exactpro.th2.common.metrics.MetricMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Request with ability retry.
 * @param <T> request result
 */
public abstract class RetryRequest<T> implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryRequest.class);
    private static final Supplier<Channel> DEFAULT_CHANNEL_CREATOR = () -> null;

    private final CompletableFuture<T> completableFuture = new CompletableFuture<>();
    private final Supplier<Channel> channelCreator;
    private final MetricMonitor livenessMonitor;
    private final MetricMonitor readinessMonitor;
    private final AtomicBoolean connectionIsClosed;
    private final RabbitMQConfiguration configuration;
    private final ScheduledExecutorService scheduler;

    private int attemptCount = 0;
    private Channel prevChannel = null;
    private Channel channel;

    public RetryRequest(@NotNull Supplier<Channel> channelCreator,
            AtomicBoolean connectionIsClosed,
            RabbitMQConfiguration configuration,
            ScheduledExecutorService scheduler,
            MetricMonitor livenessMonitor,
            MetricMonitor readinessMonitor) {
        this.channelCreator = Objects.requireNonNull(channelCreator, "Channel creator can not be null");
        this.connectionIsClosed = connectionIsClosed;
        this.configuration = configuration;
        this.scheduler = scheduler;
        this.livenessMonitor = livenessMonitor;
        this.readinessMonitor = readinessMonitor;
    }

    public RetryRequest(AtomicBoolean connectionIsClosed,
            RabbitMQConfiguration configuration,
            ScheduledExecutorService scheduler,
            MetricMonitor livenessMonitor,
            MetricMonitor readinessMonitor) {
        this(DEFAULT_CHANNEL_CREATOR, connectionIsClosed, configuration, scheduler, livenessMonitor, readinessMonitor);
    }

    @Override
    public void run() {
        if (connectionIsClosed.get()) {
            completableFuture.cancel(true);
            return;
        }

        if (channel == null) {
            Channel newChannel = channelCreator.get();
            if (newChannel != null && newChannel == prevChannel) {
                completableFuture.completeExceptionally(new CreateChannelException("Can not create channel"));
                return;
            }
            channel = newChannel;
        }

        try {
            try {
                T result = action(channel);
                completableFuture.complete(result);
                return;
            } finally {
                readinessMonitor.enable();
                livenessMonitor.enable();
            }
        } catch (IOException e) {
            readinessMonitor.disable();
            LOGGER.warn("Can not execute retry request for RabbitMQ", e);

            if (attemptCount >= configuration.getMaxRecoveryAttempts()) {
                livenessMonitor.disable();
            }
        } catch (AlreadyClosedException e) {
            LOGGER.warn("Channel is closed from retry request for RabbitMQ", e);

            if (channel != null) {
                try {
                    channel.abort();
                } catch (IOException ex) {
                    LOGGER.warn("Can not abort channel from retry request for RabbitMQ", ex);
                }

                prevChannel = channel;
                channel = null;
            }
        } catch (Exception e) {
            completableFuture.completeExceptionally(e);
        }

        if (!completableFuture.isCancelled()) {
            int nextDelay = getNextDelay();
            LOGGER.debug("Retry RabbitMQ request. Count of try = '{}'. Time for wait = '{}'", attemptCount, nextDelay);
            scheduler.schedule(this, nextDelay, TimeUnit.MILLISECONDS);
        }
    }

    public CompletableFuture<T> getCompletableFuture() {
        return completableFuture;
    }

    protected abstract T action(Channel channel) throws IOException, AlreadyClosedException;

    private int getNextDelay() {
        if (attemptCount < configuration.getMaxRecoveryAttempts()) {
            attemptCount++;
        }
        return (int) ((double)configuration.getMaxConnectionRecoveryTimeout()) / configuration.getMaxRecoveryAttempts() * attemptCount;
    }
}

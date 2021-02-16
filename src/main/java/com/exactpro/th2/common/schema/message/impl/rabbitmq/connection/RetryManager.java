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

import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.metrics.MetricArbiter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Managed all retries. Create {@link RetryRequest}
 */
public class RetryManager {

    private final RabbitMQConfiguration configuration;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean connectionIsClosed;
    private final ExecutorService tasker;
    private final Supplier<Channel> channelCreator;

    public RetryManager(@NotNull RabbitMQConfiguration configuration, @NotNull ScheduledExecutorService scheduler, @NotNull ExecutorService tasker, @NotNull Supplier<Channel> channelCreator, @NotNull AtomicBoolean connectionIsClosed) {
        this.configuration = Objects.requireNonNull(configuration, "Configuration can not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "Scheduler can not be null");
        this.tasker = Objects.requireNonNull(tasker, "Tasker can not be null");
        this.channelCreator = Objects.requireNonNull(channelCreator, "Channel creator can not be null");
        this.connectionIsClosed = Objects.requireNonNull(connectionIsClosed, "Connection checker can not be null");
    }

    public <T> CompletableFuture<T> withRetry(RetryFunction<T> func, HealthMetrics metrics) {
        return withRetry(func, null, channelCreator, metrics.getLivenessMonitor(), metrics.getReadinessMonitor());
    }

    public <T> CompletableFuture<T> withRetry(RetryFunction<T> func, Channel channel, HealthMetrics metrics) {
        return withRetry(func, channel, channelCreator, metrics.getLivenessMonitor(), metrics.getReadinessMonitor());
    }

    public <T> CompletableFuture<T> withRetry(RetryFunction<T> func, Channel channel, MetricArbiter.MetricMonitor liveness, MetricArbiter.MetricMonitor readness) {
        return withRetry(func, channel, channelCreator, liveness, readness);
    }

    public <T> CompletableFuture<T> withRetry(RetryFunction<T> func, Channel channel, @NotNull Supplier<Channel> channelCreator, MetricArbiter.MetricMonitor liveness, MetricArbiter.MetricMonitor readness) {
        RetryRequest<T> retryRequest = new RetryRequest<>(channel, channelCreator, connectionIsClosed, configuration, scheduler, tasker, liveness, readness) {
            @Override
            protected T action(Channel channel) throws IOException, AlreadyClosedException {
                return func.apply(Objects.requireNonNull(channel, "Channel can not be null"));
            }
        };
        tasker.execute(retryRequest);
        return retryRequest.getCompletableFuture();
    }

    public CompletableFuture<Void> withRetry(RetryAction action, HealthMetrics metrics) {
        return withRetry(action, null, channelCreator, metrics.getLivenessMonitor(), metrics.getReadinessMonitor());
    }

    public CompletableFuture<Void> withRetry(RetryAction action, Channel channel, HealthMetrics metrics) {
        return withRetry(action, channel, channelCreator, metrics.getLivenessMonitor(), metrics.getReadinessMonitor());
    }

    public CompletableFuture<Void> withRetry(RetryAction action, Channel channel, MetricArbiter.MetricMonitor liveness, MetricArbiter.MetricMonitor readness) {
        return withRetry(action, channel, channelCreator, liveness, readness);
    }

    public CompletableFuture<Void> withRetry(RetryAction action, Channel channel, @NotNull Supplier<Channel> channelCreator, MetricArbiter.MetricMonitor liveness, MetricArbiter.MetricMonitor readness) {
        RetryRequest<Void> retryRequest = new RetryRequest<Void>(channel, channelCreator, connectionIsClosed, configuration, scheduler, tasker, liveness, readness) {
            @Override
            protected Void action(Channel channel) throws IOException, AlreadyClosedException {
                action.apply(Objects.requireNonNull(channel, "Channel can not be null"));
                return null;
            }
        };
        tasker.execute(retryRequest);
        return retryRequest.getCompletableFuture();
    }

    public interface RetryFunction<T> {
        T apply(@NotNull Channel channel) throws IOException, AlreadyClosedException;
    }

    public interface RetryAction {
        void apply(@NotNull Channel channel) throws IOException, AlreadyClosedException;
    }

}

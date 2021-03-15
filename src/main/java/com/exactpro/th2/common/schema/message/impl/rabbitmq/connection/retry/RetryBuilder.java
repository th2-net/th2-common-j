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

import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.metrics.MetricMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class RetryBuilder<T> {

    private final RabbitMQConfiguration configuration;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean connectionIsClosed;
    private final ExecutorService tasker;

    private RetryFunction<T> function = null;
    private RetryAction action = null;
    private Channel channel = null;
    private MetricMonitor liveness = null;
    private MetricMonitor readness = null;
    private Supplier<Channel> channelCreator = null;
    private long time = -1;
    private TimeUnit unit;

    public RetryBuilder(@NotNull RabbitMQConfiguration configuration, @NotNull ScheduledExecutorService scheduler, @NotNull ExecutorService tasker, @NotNull AtomicBoolean connectionIsClosed) {
        this.configuration = Objects.requireNonNull(configuration, "Configuration can not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "Scheduler can not be null");
        this.tasker = Objects.requireNonNull(tasker, "Tasker can not be null");
        this.connectionIsClosed = Objects.requireNonNull(connectionIsClosed, "Connection checker can not be null");
    }

    public RetryBuilder<T> setFunction(RetryFunction<T> function) {
        this.function = function;
        return this;
    }

    public RetryBuilder<T> setAction(RetryAction action) {
        this.action = action;
        return this;
    }

    public RetryBuilder<T> setChannel(Channel channel) {
        this.channel = channel;
        return this;
    }

    public RetryBuilder<T> setLiveness(MetricMonitor liveness) {
        this.liveness = liveness;
        return this;
    }

    public RetryBuilder<T> setReadness(MetricMonitor readness) {
        this.readness = readness;
        return this;
    }

    public RetryBuilder<T> setMetrics(HealthMetrics metrics) {
        setLiveness(metrics.getLivenessMonitor());
        setReadness(metrics.getReadinessMonitor());
        return this;
    }

    public RetryBuilder<T> setChannelCreator(Supplier<Channel> channelCreator) {
        this.channelCreator = channelCreator;
        return this;
    }

    public RetryBuilder<T> setDelay(long time, TimeUnit unit) {
        this.time = time;
        this.unit = unit;
        return this;
    }

    public CompletableFuture<CompletableFuture<T>> createWithFunc() throws IllegalStateException {
        if (function == null) {
            throw new IllegalStateException("Function or action should be not null");
        }

        RetryRequest<T> request = new RetryRequest<T>(channel, channelCreator, connectionIsClosed, configuration, scheduler, tasker, liveness, readness) {
            @Override
            protected T action(Channel channel) throws IOException, AlreadyClosedException {
                return function.apply(channel);
            }
        };

        return executeRetry(request);
    }

    public CompletableFuture<T> createWithFuncNow() throws IllegalStateException {
        if (time > 0 || unit != null) {
            throw new IllegalStateException("Can not create retry request now, because it has delay");
        }

        try {
            return createWithFunc().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Can not create retry request", e);
        }
    }

    public CompletableFuture<CompletableFuture<Void>> createWithAction() throws IllegalStateException {
        if (action == null) {
            throw new IllegalStateException("Function or action should be not null");
        }

        RetryRequest<Void> request = new RetryRequest<Void>(channel, channelCreator, connectionIsClosed, configuration, scheduler, tasker, liveness, readness) {
            @Override
            protected Void action(Channel channel) throws IOException, AlreadyClosedException {
                action.apply(channel);
                return null;
            }
        };

        return executeRetry(request);
    }

    public CompletableFuture<Void> createWithActionNow() throws IllegalStateException {
        if (time > 0 || unit != null) {
            throw new IllegalStateException("Can not create retry request now, because it has delay");
        }

        try {
            return createWithAction().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Can not create retry request", e);
        }
    }

    public CompletableFuture<CompletableFuture<?>> create() throws IllegalStateException {
        if (function == null && action == null) {
            throw new IllegalStateException("Function or action should be not null");
        }

        RetryRequest<?> request = null;
        if (function != null) {
            request = new RetryRequest<T>(channel, channelCreator, connectionIsClosed, configuration, scheduler, tasker, liveness, readness) {
                @Override
                protected T action(Channel channel) throws IOException, AlreadyClosedException {
                    return function.apply(channel);
                }
            };
        } else {
            request = new RetryRequest<Void>(channel, channelCreator, connectionIsClosed, configuration, scheduler, tasker, liveness, readness) {
                @Override
                protected Void action(Channel channel) throws IOException, AlreadyClosedException {
                    action.apply(channel);
                    return null;
                }
            };
        }

        CompletableFuture<CompletableFuture<?>> future = new CompletableFuture<>();

        if (time > 0 && unit != null) {
            RetryRequest<?> finalRequest = request;
            scheduler.schedule(() -> {
                tasker.execute(finalRequest);
                future.complete(finalRequest.getCompletableFuture());
            }, time, unit);
        } else {
            tasker.execute(request);
            future.complete(request.getCompletableFuture());
        }

        return future;
    }

    public CompletableFuture<?> createNow() throws IllegalStateException {
        if (time > 0 || unit != null) {
            throw new IllegalStateException("Can not create retry request now, because it has delay");
        }

        try {
            return create().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Can not create retry request", e);
        }
    }

    private <R> CompletableFuture<CompletableFuture<R>> executeRetry(RetryRequest<R> request) {
        CompletableFuture<CompletableFuture<R>> future = new CompletableFuture<>();

        if (time > 0 && unit != null) {
            scheduler.schedule(() -> {
                tasker.execute(request);
                future.complete(request.getCompletableFuture());
            }, time, unit);
        } else {
            tasker.execute(request);
            future.complete(request.getCompletableFuture());
        }

        return future;
    }
}

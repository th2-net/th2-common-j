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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.metrics.MetricMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;

public class RetryBuilder<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryBuilder.class);

    private final RabbitMQConfiguration configuration;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean connectionIsClosed;
    private final Supplier<Channel> channelCreator;

    private MetricMonitor liveness = null;
    private MetricMonitor readness = null;
    private long time = -1;
    private TimeUnit unit;

    public RetryBuilder(@NotNull RabbitMQConfiguration configuration, @NotNull ScheduledExecutorService scheduler,
            @NotNull AtomicBoolean connectionIsClosed, @NotNull Supplier<Channel> channelCreator) {
        this.configuration = requireNonNull(configuration, "Configuration can not be null");
        this.scheduler = requireNonNull(scheduler, "Scheduler can not be null");
        this.connectionIsClosed = requireNonNull(connectionIsClosed, "Connection checker can not be null");
        this.channelCreator = requireNonNull(channelCreator, "Channel creator can not be null");
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

    public RetryBuilder<T> setDelay(long time, TimeUnit unit) {
        this.time = time;
        this.unit = unit;
        return this;
    }

    public CompletableFuture<T> build(RetryFunction<T> function) throws IllegalStateException {
        requireNonNull(function, "Function or action should be not null");

        RetryRequest<T> request = new RetryRequest<T>(channelCreator, connectionIsClosed, configuration, scheduler, liveness, readness) {
            @Override
            protected T action(Channel channel) throws IOException, AlreadyClosedException {
                return function.apply(channel);
            }
        };

        return executeRetry(request);
    }

    private <R> CompletableFuture<R> executeRetry(RetryRequest<R> request) {
        CompletableFuture<R> future = request.getCompletableFuture();

        if (time > 0 && unit != null) {
            scheduler.schedule(request, time, unit);
        } else {
            scheduler.submit(request);
        }

        return future;
    }
}

/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.event;

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSubscriber;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.rabbitmq.client.Delivery;

import static com.exactpro.th2.common.metrics.CommonMetrics.TH2_PIN_LABEL;
import static com.exactpro.th2.common.schema.event.EventBatchRouter.EVENT_TYPE;
import io.prometheus.client.Counter;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.exactpro.th2.common.message.MessageUtils.toJson;

public class EventBatchSubscriber extends AbstractRabbitSubscriber<EventBatch> {
    private static final Counter EVENT_SUBSCRIBE_TOTAL = Counter.build()
            .name("th2_event_subscribe_total")
            .labelNames(TH2_PIN_LABEL)
            .help("Quantity of received events")
            .register();

    public EventBatchSubscriber(
            @NotNull ConnectionManager connectionManager,
            @NotNull String queue,
            @NotNull FilterFunction filterFunc,
            @NotNull String th2Pin
    ) {
        super(connectionManager, queue, filterFunc, th2Pin, EVENT_TYPE);
    }

    @Override
    protected EventBatch valueFromBytes(byte[] bytes) throws Exception {
        return EventBatch.parseFrom(bytes);
    }

    @Override
    protected String toShortTraceString(EventBatch value) {
        return toJson(value);
    }

    @Override
    protected String toShortDebugString(EventBatch value) {
        return "EventBatch: parent_event_id = " + value.getParentEventId().getId();
    }

    @Nullable
    @Override
    protected EventBatch filter(EventBatch eventBatch) throws Exception {
        return eventBatch;
    }

    @Override
    protected void handle(String consumeTag, Delivery delivery, EventBatch value) {
        EVENT_SUBSCRIBE_TOTAL
                .labels(th2Pin)
                .inc(value.getEventsCount());
        super.handle(consumeTag, delivery, value);
    }
}

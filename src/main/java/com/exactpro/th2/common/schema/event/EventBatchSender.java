/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.PublishConnectionManager;

import static com.exactpro.th2.common.metrics.CommonMetrics.TH2_PIN_LABEL;
import static com.exactpro.th2.common.schema.event.EventBatchRouter.EVENT_TYPE;

import io.prometheus.client.Counter;

public class EventBatchSender extends AbstractRabbitSender<EventBatch> {
    private static final Counter EVENT_PUBLISH_TOTAL = Counter.build()
            .name("th2_event_publish_total")
            .labelNames(TH2_PIN_LABEL)
            .help("Quantity of published events")
            .register();

    public EventBatchSender(
            @NotNull PublishConnectionManager publishConnectionManager,
            @NotNull String exchangeName,
            @NotNull String routingKey,
            @NotNull String th2Pin,
            @NotNull String bookName
    ) {
        super(publishConnectionManager, exchangeName, routingKey, th2Pin, EVENT_TYPE, bookName);
    }

    @Override
    public void send(EventBatch value) throws IOException {
        EVENT_PUBLISH_TOTAL
                .labels(th2Pin)
                .inc(value.getEventsCount());
        if (value.getEventsList().stream().anyMatch(event -> event.getId().getBookName().isEmpty())) {
            EventBatch.Builder eventBatchBuilder = EventBatch.newBuilder();
            value.getEventsList().forEach(event -> {
                Event.Builder eventBuilder = event.toBuilder();
                if (event.getId().getBookName().isEmpty()) {
                    eventBuilder.getIdBuilder().setBookName(bookName);
                }
                eventBatchBuilder.addEvents(eventBuilder);
            });
            super.send(eventBatchBuilder.build());
        } else {
            super.send(value);
        }
    }

    @Override
    protected byte[] valueToBytes(EventBatch value) {
        return value.toByteArray();
    }

    @Override
    protected String toShortTraceString(EventBatch value) {
        return MessageUtils.toJson(value);
    }

    @Override
    protected String toShortDebugString(EventBatch value) {
        return "EventBatch: parent_event_id = " + value.getParentEventId().getId();
    }
}
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
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSubscriber;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static com.exactpro.th2.common.message.MessageUtils.toJson;
import static com.exactpro.th2.common.metrics.CommonMetrics.DEFAULT_BUCKETS;

public class EventBatchSubscriber extends AbstractRabbitSubscriber<EventBatch> {

    private static final Counter INCOMING_EVENT_BATCH_QUANTITY = Counter.build()
            .name("th2_mq_incoming_event_batch_quantity")
            .help("Quantity of incoming event batches")
            .register();
    private static final Counter INCOMING_EVENT_QUANTITY = Counter.build()
            .name("th2_mq_incoming_event_quantity")
            .help("Quantity of incoming events")
            .register();
    private static final Histogram EVENT_PROCESSING_TIME = Histogram.build()
            .buckets(DEFAULT_BUCKETS)
            .name("th2_mq_event_processing_time")
            .help("Time of processing events")
            .register();
    private static final String[] NO_LABELS = {};

    @Override
    protected Counter getDeliveryCounter() {
        return INCOMING_EVENT_BATCH_QUANTITY;
    }

    @Override
    protected Counter getContentCounter() {
        return INCOMING_EVENT_QUANTITY;
    }

    @Override
    protected Histogram getProcessingTimer() {
        return EVENT_PROCESSING_TIME;
    }

    @Override
    protected String[] extractLabels(EventBatch batch) {
        return NO_LABELS;
    }

    @Override
    protected int extractCountFrom(EventBatch batch) {
        return batch.getEventsCount();
    }

    @Override
    protected List<EventBatch> valueFromBytes(byte[] bytes) throws Exception {
        return List.of(EventBatch.parseFrom(bytes));
    }

    @Override
    protected String toShortTraceString(EventBatch value) {
        return toJson(value);
    }

    @Override
    protected String toShortDebugString(EventBatch value) {
        return String.format("EventBatch: parent_event_id = %s", value.getParentEventId().getId());
    }

    @Nullable
    @Override
    protected EventBatch filter(EventBatch eventBatch) throws Exception {
        return eventBatch;
    }
}

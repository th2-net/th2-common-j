/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.common.schema.event;

import java.util.Set;

import org.apache.commons.collections4.SetUtils;
import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.MessageSubscriber;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.google.protobuf.TextFormat;

import io.prometheus.client.Counter;

public class EventBatchRouter extends AbstractRabbitRouter<EventBatch> {
    private static final Set<String> REQUIRED_SUBSCRIBE_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.EVENT.toString(), QueueAttribute.SUBSCRIBE.toString());
    private static final Set<String> REQUIRED_SEND_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.EVENT.toString(), QueueAttribute.PUBLISH.toString());

    private static final Counter OUTGOING_EVENT_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_event_batch_quantity", "Quantity of outgoing event batches").register();
    private static final Counter OUTGOING_EVENT_QUANTITY = Counter.build("th2_mq_outgoing_event_quantity", "Quantity of outgoing events").register();

    @NotNull
    @Override
    protected EventBatch splitAndFilter(EventBatch message, @NotNull QueueConfiguration pinConfiguration) {
        return message;
    }

    @NotNull
    @Override
    protected Set<String> getRequiredSendAttributes() {
        return REQUIRED_SEND_ATTRIBUTES;
    }

    @NotNull
    @Override
    protected Set<String> getRequiredSubscribeAttributes() {
        return REQUIRED_SUBSCRIBE_ATTRIBUTES;
    }

    @NotNull
    @Override
    protected MessageSender<EventBatch> createSender(QueueConfiguration queueConfiguration) {
        EventBatchSender eventBatchSender = new EventBatchSender();
        eventBatchSender.init(
                getConnectionManager(),
                queueConfiguration.getExchange(),
                queueConfiguration.getRoutingKey()
        );
        return eventBatchSender;
    }

    @NotNull
    @Override
    protected MessageSubscriber<EventBatch> createSubscriber(QueueConfiguration queueConfiguration) {
        EventBatchSubscriber eventBatchSubscriber = new EventBatchSubscriber();
        eventBatchSubscriber.init(
                getConnectionManager(),
                new SubscribeTarget(queueConfiguration.getQueue(), queueConfiguration.getRoutingKey(), queueConfiguration.getExchange()),
                FilterFunction.DEFAULT_FILTER_FUNCTION
        );
        return eventBatchSubscriber;
    }

    @NotNull
    @Override
    protected String toErrorString(EventBatch eventBatch) {
        return TextFormat.shortDebugString(eventBatch);
    }

    @NotNull
    @Override
    protected Counter getDeliveryCounter() {
        return OUTGOING_EVENT_BATCH_QUANTITY;
    }

    @NotNull
    @Override
    protected Counter getContentCounter() {
        return OUTGOING_EVENT_QUANTITY;
    }

    @Override
    protected int extractCountFrom(EventBatch batch) {
        return batch.getEventsCount();
    }
}

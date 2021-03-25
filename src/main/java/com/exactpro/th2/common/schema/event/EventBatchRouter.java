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

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.MessageQueue;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitMessageRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.google.protobuf.Message;
import org.apache.commons.collections4.SetUtils;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class EventBatchRouter extends AbstractRabbitMessageRouter<EventBatch> {

    private static final Set<String> REQUIRED_SUBSCRIBE_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.EVENT.toString(), QueueAttribute.SUBSCRIBE.toString());
    private static final Set<String> REQUIRED_SEND_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.EVENT.toString(), QueueAttribute.PUBLISH.toString());

    @Override
    protected MessageQueue<EventBatch> createQueue(@NotNull ConnectionManager connectionManager, @NotNull QueueConfiguration queueConfiguration, @NotNull FilterFunction filterFunction) {
        EventBatchQueue eventBatchQueue = new EventBatchQueue();
        eventBatchQueue.init(connectionManager, queueConfiguration, filterFunction);
        return eventBatchQueue;
    }

    @Override
    protected Map<String, EventBatch> findQueueByFilter(Map<String, QueueConfiguration> queues, EventBatch msg) {
        return queues.entrySet().stream().collect(Collectors.toMap(Entry::getKey, v -> msg));
    }

    @Override
    protected boolean filterMessage(Message msg, List<? extends RouterFilter> filters) {
        return true;
    }

    @Override
    protected Set<String> requiredSubscribeAttributes() {
        return REQUIRED_SUBSCRIBE_ATTRIBUTES;
    }

    @Override
    protected Set<String> requiredSendAttributes() {
        return REQUIRED_SEND_ATTRIBUTES;
    }
}

/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.schema.event;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.SetUtils;

import com.exactpro.th2.infra.grpc.EventBatch;
import com.exactpro.th2.schema.message.MessageQueue;
import com.exactpro.th2.schema.message.QueueAttribute;
import com.exactpro.th2.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.AbstractRabbitMessageRouter;
import com.exactpro.th2.schema.message.impl.rabbitmq.connection.ConnectionManager;

public class EventBatchRouter extends AbstractRabbitMessageRouter<EventBatch> {

    private static final Set<String> REQUIRED_SUBSCRIBE_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.EVENT.toString(), QueueAttribute.SUBSCRIBE.toString());
    private static final Set<String> REQUIRED_SEND_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.EVENT.toString(), QueueAttribute.PUBLISH.toString());

    @Override
    protected MessageQueue<EventBatch> createQueue(ConnectionManager connectionManager, QueueConfiguration queueConfiguration) {
        EventBatchQueue eventBatchQueue = new EventBatchQueue();
        eventBatchQueue.init(connectionManager, queueConfiguration);
        return eventBatchQueue;
    }

    @Override
    protected Map<String, EventBatch> findByFilter(Map<String, QueueConfiguration> queues, EventBatch msg) {
        return queues.entrySet().stream().collect(Collectors.toMap(Entry::getKey, v -> msg));
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

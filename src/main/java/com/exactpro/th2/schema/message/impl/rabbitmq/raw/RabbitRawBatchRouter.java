/*****************************************************************************
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
 *****************************************************************************/

package com.exactpro.th2.schema.message.impl.rabbitmq.raw;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.SetUtils;
import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.infra.grpc.RawMessage;
import com.exactpro.th2.infra.grpc.RawMessageBatch;
import com.exactpro.th2.infra.grpc.RawMessageBatch.Builder;
import com.exactpro.th2.schema.message.MessageQueue;
import com.exactpro.th2.schema.message.QueueAttribute;
import com.exactpro.th2.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.exactpro.th2.schema.message.impl.rabbitmq.router.AbstractRabbitBatchMessageRouter;

public class RabbitRawBatchRouter extends AbstractRabbitBatchMessageRouter<RawMessage, RawMessageBatch, RawMessageBatch.Builder> {

    private static final Set<String> requiredSubscribeAttribute = SetUtils.unmodifiableSet(QueueAttribute.RAW.toString(), QueueAttribute.SUBSCRIBE.toString());
    private static final Set<String> requiredSendAttributes = SetUtils.unmodifiableSet(QueueAttribute.RAW.toString(), QueueAttribute.PUBLISH.toString());

    @Override
    protected MessageQueue<RawMessageBatch> createQueue(@NotNull ConnectionManager connectionManager, QueueConfiguration queueConfiguration) {
        MessageQueue<RawMessageBatch> queue = new RabbitRawBatchQueue();
        queue.init(connectionManager, queueConfiguration);
        return queue;
    }

    @Override
    protected Set<String> requiredSubscribeAttributes() {
        return requiredSubscribeAttribute;
    }

    @Override
    protected Set<String> requiredSendAttributes() {
        return requiredSendAttributes;
    }

    @Override
    protected List<RawMessage> getMessages(RawMessageBatch batch) {
        return batch.getMessagesList();
    }

    @Override
    protected Builder createBatchBuilder() {
        return RawMessageBatch.newBuilder();
    }

    @Override
    protected void addMessage(Builder builder, RawMessage message) {
        builder.addMessages(message);
    }

    @Override
    protected RawMessageBatch build(Builder builder) {
        return builder.build();
    }
}

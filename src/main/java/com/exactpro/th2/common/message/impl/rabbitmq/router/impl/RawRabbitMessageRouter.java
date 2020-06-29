/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.message.impl.rabbitmq.router.impl;

import com.exactpro.th2.common.message.MessageQueue;
import com.exactpro.th2.common.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.common.message.impl.rabbitmq.raw.RabbitRawBatchQueue;
import com.exactpro.th2.common.message.impl.rabbitmq.router.AbstractRabbitBatchMessageRouter;
import com.exactpro.th2.infra.grpc.RawMessage;
import com.exactpro.th2.infra.grpc.RawMessageBatch;

import java.util.List;

public class RawRabbitMessageRouter extends AbstractRabbitBatchMessageRouter<RawMessage, RawMessageBatch> {

    @Override
    protected MessageQueue<RawMessageBatch> createQueue(RabbitMQConfiguration configuration, QueueConfiguration queueConfiguration) {
        RabbitRawBatchQueue queue = new RabbitRawBatchQueue();
        queue.init(configuration, queueConfiguration);
        return queue;
    }

    @Override
    protected List<RawMessage> crackBatch(RawMessageBatch batch) {
        return batch.getMessagesList();
    }

    @Override
    protected RawMessageBatch createBatch() {
        return RawMessageBatch.newBuilder().build();
    }

}

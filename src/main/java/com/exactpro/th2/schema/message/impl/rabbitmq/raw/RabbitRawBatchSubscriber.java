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

import com.exactpro.th2.infra.grpc.RawMessage;
import com.exactpro.th2.infra.grpc.RawMessageBatch;
import com.exactpro.th2.schema.filter.strategy.FilterStrategy;
import com.exactpro.th2.schema.message.configuration.RouterFilter;
import com.exactpro.th2.schema.message.impl.rabbitmq.AbstractRabbitBatchSubscriber;

import java.util.List;

public class RabbitRawBatchSubscriber extends AbstractRabbitBatchSubscriber<RawMessage, RawMessageBatch> {

    private static final String MESSAGE_TYPE = "raw";


    public RabbitRawBatchSubscriber(List<? extends RouterFilter> filters) {
        super(filters);
    }

    public RabbitRawBatchSubscriber(List<? extends RouterFilter> filters, FilterStrategy filterStrategy) {
        super(filters, filterStrategy);
    }


    @Override
    protected RawMessageBatch valueFromBytes(byte[] body) throws Exception {
        return RawMessageBatch.parseFrom(body);
    }

    @Override
    protected List<RawMessage> getMessages(RawMessageBatch batch) {
        return batch.getMessagesList();
    }

    @Override
    protected RawMessageBatch createBatch(List<RawMessage> messages) {
        return RawMessageBatch.newBuilder().addAllMessages(messages).build();
    }

    @Override
    protected Metadata extractMetadata(RawMessage message) {
        var metadata = message.getMetadata();
        var messageID = metadata.getId();
        return Metadata.builder()
                .messageType(MESSAGE_TYPE)
                .direction(messageID.getDirection())
                .sequence(messageID.getSequence())
                .sessionAlias(messageID.getConnectionId().getSessionAlias())
                .build();
    }

}

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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.raw;

import java.util.stream.Collectors;

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender;

import static com.exactpro.th2.common.message.MessageUtils.getDebugString;

public class RabbitRawBatchSender extends AbstractRabbitSender<RawMessageBatch> {

    @Override
    protected byte[] valueToBytes(RawMessageBatch value) {
        var batchBuilder = MessageGroupBatch.newBuilder();

        for (var rawMessage : value.getMessagesList()) {
            var anyMessage = AnyMessage.newBuilder().setRawMessage(rawMessage).build();
            var group = MessageGroup.newBuilder().addMessages(anyMessage).build();
            batchBuilder.addGroups(group);
        }

        return batchBuilder.build().toByteArray();
    }

    @Override
    protected String toShortTraceString(RawMessageBatch value) {
        return MessageUtils.toJson(value);
    }

    @Override
    protected String toShortDebugString(RawMessageBatch value) {
        return getDebugString(getClass().getSimpleName(),
                value.getMessagesList().stream().map(message -> message.getMetadata().getId()).collect(Collectors.toList()));
    }
}

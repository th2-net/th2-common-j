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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.parsed;

import static com.exactpro.th2.common.message.MessageUtils.getDebugString;

import java.util.stream.Collectors;

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender;

public class RabbitParsedBatchSender extends AbstractRabbitSender<MessageBatch> {

    @Override
    protected byte[] valueToBytes(MessageBatch value) {
        var groupBuilder = MessageGroup.newBuilder();

        for (var message : value.getMessagesList()) {
            var anyMessage = AnyMessage.newBuilder().setMessage(message).build();
            groupBuilder.addMessages(anyMessage);
        }

        var batchBuilder = MessageGroupBatch.newBuilder();
        var group = groupBuilder.build();

        return batchBuilder.addGroups(group).build().toByteArray();
    }

    @Override
    protected String toShortTraceString(MessageBatch value) {
        return MessageUtils.toJson(value);
    }

    @Override
    protected String toShortDebugString(MessageBatch value) {
        return getDebugString(getClass().getSimpleName(),
                value.getMessagesList().stream().map(message -> message.getMetadata().getId()).collect(Collectors.toList()));
    }
}

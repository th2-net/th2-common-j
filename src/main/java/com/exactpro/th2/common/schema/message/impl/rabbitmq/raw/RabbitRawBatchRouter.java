/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import java.util.Set;

import org.apache.commons.collections4.SetUtils;
import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractGroupBatchAdapterRouter;

public class RabbitRawBatchRouter extends AbstractGroupBatchAdapterRouter<RawMessageBatch> {
    private static final Set<String> REQUIRED_SUBSCRIBE_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.RAW.toString(), QueueAttribute.SUBSCRIBE.toString());
    private static final Set<String> REQUIRED_SEND_ATTRIBUTES = SetUtils.unmodifiableSet(QueueAttribute.RAW.toString(), QueueAttribute.PUBLISH.toString());

    @NotNull
    @Override
    public Set<String> getRequiredSendAttributes() {
        return REQUIRED_SEND_ATTRIBUTES;
    }

    @NotNull
    @Override
    public Set<String> getRequiredSubscribeAttributes() {
        return REQUIRED_SUBSCRIBE_ATTRIBUTES;
    }

    @Override
    protected @NotNull MessageGroupBatch buildGroupBatch(RawMessageBatch rawMessageBatch) {
        var messageGroupBatchBuilder = MessageGroupBatch.newBuilder();
        rawMessageBatch.getMessagesList().forEach(rawMessage ->
                messageGroupBatchBuilder.addGroups(
                        MessageGroup.newBuilder().addMessages(AnyMessage.newBuilder().setRawMessage(rawMessage).build())
                )
        );
        return messageGroupBatchBuilder.build();
    }

    @Override
    protected RawMessageBatch buildFromGroupBatch(@NotNull MessageGroupBatch groupBatch) {
        var builder = RawMessageBatch.newBuilder();
        groupBatch.getGroupsList().stream()
                .flatMap(messageGroup -> messageGroup.getMessagesList().stream())
                .peek(anyMessage -> {
                    if (!anyMessage.hasRawMessage()) {
                        throw new IllegalStateException("Message group batch contains not raw message: " + MessageUtils.toJson(groupBatch));
                    }
                })
                .map(AnyMessage::getRawMessage)
                .forEach(builder::addMessages);
        return builder.build();
    }
}

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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.parsed;

import com.exactpro.th2.common.schema.message.MessageRouterContext;
import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.schema.message.MessageRouterUtils;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;

import io.prometheus.client.Counter;

public class RabbitParsedBatchSender extends AbstractRabbitSender<MessageBatch> {

    private static final Counter OUTGOING_PARSED_MSG_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_parsed_msg_batch_quantity", "Quantity of outgoing parsed message batches").register();
    private static final Counter OUTGOING_PARSED_MSG_QUANTITY = Counter.build("th2_mq_outgoing_parsed_msg_quantity", "Quantity of outgoing parsed messages").register();

    public RabbitParsedBatchSender(@NotNull MessageRouterContext context, @NotNull String exchangeName, @NotNull String routingKey) {
        super(context, exchangeName, routingKey);
    }

    @Override
    protected Counter getDeliveryCounter() {
        return OUTGOING_PARSED_MSG_BATCH_QUANTITY;
    }

    @Override
    protected Counter getContentCounter() {
        return OUTGOING_PARSED_MSG_QUANTITY;
    }

    @Override
    protected int extractCountFrom(MessageBatch message) {
        return message.getMessagesCount();
    }

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
    protected String toShortDebugString(MessageBatch value) {
        return MessageRouterUtils.toJson(value);
    }
}

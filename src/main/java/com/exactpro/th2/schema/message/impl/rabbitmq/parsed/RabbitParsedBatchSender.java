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

package com.exactpro.th2.schema.message.impl.rabbitmq.parsed;

import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.schema.message.impl.rabbitmq.AbstractRabbitSender;
import com.google.protobuf.TextFormat;

public class RabbitParsedBatchSender extends AbstractRabbitSender<MessageBatch> {
    @Override
    protected byte[] valueToBytes(MessageBatch value) {
        return value.toByteArray();
    }

    @Override
    protected String toShortDebugString(MessageBatch value) {
        return TextFormat.shortDebugString(value);
    }
}

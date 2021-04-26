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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.listener;

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.schema.message.MessageListener;

public class AnyMessageSortListener extends SortListener<AnyMessage> {
    public AnyMessageSortListener(MessageListener<AnyMessage> delegate, int bufferSize) {
        super(delegate, bufferSize);
    }

    public AnyMessageSortListener(MessageListener<AnyMessage> delegate) {
        super(delegate);
    }

    @Override
    protected long getSequence(AnyMessage message) {
        return message.getKindCase() == AnyMessage.KindCase.MESSAGE ? message.getMessage().getMetadata().getId().getSequence() : message.getRawMessage().getMetadata().getId().getSequence();
    }
}

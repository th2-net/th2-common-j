/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.message.impl;

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.Direction;
import com.exactpro.th2.common.message.MessageIdBuilder;

public class MessageIdBuilderImpl implements MessageIdBuilder<MessageIdBuilderImpl, MessageID> {
    private final MessageID.Builder idBuilder = MessageID.newBuilder();

    @Override
    public MessageIdBuilderImpl setSessionAlias(String alias) {
        idBuilder.getConnectionIdBuilder().setSessionAlias(alias);
        return this;
    }

    @Override
    public MessageIdBuilderImpl setDirection(Direction direction) {
        idBuilder.setDirection(com.exactpro.th2.common.grpc.Direction.forNumber(direction.getValue()));
        return this;
    }

    @Override
    public MessageIdBuilderImpl setSequence(long sequence) {
        idBuilder.setSequence(sequence);
        return this;
    }

    @Override
    public MessageIdBuilderImpl addSubsequence(int subSequence) {
        idBuilder.addSubsequence(subSequence);
        return this;
    }

    @Override
    public MessageID build() {
        return idBuilder.build();
    }
}

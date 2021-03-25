/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.common.schema.message.impl.rabbitmq;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.schema.message.FilterFunction;
import com.exactpro.th2.common.schema.message.MessageRouterContext;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.google.protobuf.Message;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public abstract class AbstractRabbitBatchSubscriber<M extends Message, MB> extends AbstractRabbitSubscriber<MB> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitBatchSubscriber.class);

    protected final List<? extends RouterFilter> filters;

    public AbstractRabbitBatchSubscriber(@NotNull MessageRouterContext context, @NotNull SubscribeTarget target, @NotNull FilterFunction filterFunction, @NotNull List<? extends RouterFilter> filters) {
        super(context, target, filterFunction);
        this.filters = filters;
    }

    @Override
    protected MB filter(MB batch) {
        if (filters.isEmpty()) {
            return batch;
        }

        var messages = new ArrayList<>(getMessages(batch));

        var each = messages.iterator();

        while (each.hasNext()) {
            var msg = each.next();
            if (!callFilterFunction(msg, filters)) {
                each.remove();
                LOGGER.debug("Message skipped because it did not satisfy filters: " + extractMetadata(msg));
            }
        }

        return messages.isEmpty() ? null : createBatch(messages);
    }


    protected abstract List<M> getMessages(MB batch);

    protected abstract MB createBatch(List<M> messages);

    protected abstract Metadata extractMetadata(M message);

    protected static class Metadata {
        private final long sequence;
        private final String messageType;
        private final Direction direction;
        private final String sessionAlias;

        public Metadata(long sequence, String messageType, Direction direction, String sessionAlias) {
            this.sequence = sequence;
            this.messageType = messageType;
            this.direction = direction;
            this.sessionAlias = sessionAlias;
        }

        public long getSequence() {
            return sequence;
        }

        public String getMessageType() {
            return messageType;
        }

        public Direction getDirection() {
            return direction;
        }

        public String getSessionAlias() {
            return sessionAlias;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("sequence", sequence)
                    .append("messageType", messageType)
                    .append("sessionAlias", sessionAlias)
                    .append("direction", direction)
                    .toString();
        }
    }

}

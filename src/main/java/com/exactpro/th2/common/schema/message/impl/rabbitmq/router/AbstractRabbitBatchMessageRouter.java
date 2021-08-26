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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.router;

import java.util.List;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitRouter;
import com.google.protobuf.Message;

public abstract class AbstractRabbitBatchMessageRouter<M extends Message, MB, MBB> extends AbstractRabbitRouter<MB> {

    @Override
    protected @NotNull MB splitAndFilter(MB batch, @NotNull QueueConfiguration pinConfiguration) {
        MBB result = createBatchBuilder();
        getMessages(batch).forEach(message -> {
            if (filter(message, pinConfiguration)) {
                addMessage(result, message);
            }
        });
        return build(result);
    }

    protected boolean filter(Message message, QueueConfiguration pinConfiguration) {
        return pinConfiguration.getFilters().isEmpty()
                || filterMessage(message, pinConfiguration.getFilters());
    }

    protected abstract List<M> getMessages(MB batch);

    protected abstract MBB createBatchBuilder();

    protected abstract void addMessage(MBB builder, M message);

    protected abstract MB build(MBB builder);

}

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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.router;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitMessageRouter;
import com.google.protobuf.Message;

public abstract class AbstractRabbitBatchMessageRouter<M extends Message, MB, MBB> extends AbstractRabbitMessageRouter<MB> {

    @Override
    protected Map<String, MB> findByFilter(Map<String, QueueConfiguration> queues, MB batch) {

        Map<String, MBB> result = new HashMap<>();

        for (var message : getMessages(batch)) {

            for (var queueAlias : filter(queues, message)) {

                result.putIfAbsent(queueAlias, createBatchBuilder());

                addMessage(result.get(queueAlias), message);

            }

        }

        return result.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, value -> build(value.getValue())));
    }

    protected Set<String> filter(Map<String, QueueConfiguration> queues, Message message) {

        var aliases = new HashSet<String>();

        for (var queueEntry : queues.entrySet()) {

            var queueAlias = queueEntry.getKey();
            var filters = queueEntry.getValue().getFilters();

            if (filters.isEmpty() || filterStrategy.get().verify(message, filters)) {
                aliases.add(queueAlias);
            }

        }

        return aliases;
    }


    protected abstract List<M> getMessages(MB batch);

    protected abstract MBB createBatchBuilder();

    protected abstract void addMessage(MBB builder, M message);

    protected abstract MB build(MBB builder);

}

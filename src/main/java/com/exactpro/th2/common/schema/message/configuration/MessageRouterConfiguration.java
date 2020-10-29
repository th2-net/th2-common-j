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

package com.exactpro.th2.common.schema.message.configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.jetbrains.annotations.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;


public class MessageRouterConfiguration {

    @Getter
    private Map<String, QueueConfiguration> queues;


    @JsonCreator(mode = Mode.PROPERTIES)
    public MessageRouterConfiguration(@JsonProperty("queues") Map<String, QueueConfiguration> queues) {
        this.queues = queues;
    }


    @Nullable
    public QueueConfiguration getQueueByAlias(String queueAlias) {
        return queues.get(queueAlias);
    }

    public Map<String, QueueConfiguration> findQueuesByAttr(String... attrs) {
        var attributes = Arrays.asList(attrs);
        return findQueuesByAttr(attributes);
    }

    public Map<String, QueueConfiguration> findQueuesByAttr(Collection<String> attr) {
        return queues.entrySet().stream()
                .filter(e -> e.getValue().getAttributes().containsAll(attr))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}

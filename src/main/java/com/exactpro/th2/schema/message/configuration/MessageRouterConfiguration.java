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

package com.exactpro.th2.schema.message.configuration;

import static java.util.Collections.emptySet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.infra.grpc.MessageFilter;
import com.exactpro.th2.infra.grpc.ValueFilter.KindCase;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;


public class MessageRouterConfiguration implements FilterableConfiguration {

    @Getter
    private Map<String, QueueConfiguration> queues;

    private Map<String, Set<String>> attributes;

    @JsonCreator(mode = Mode.PROPERTIES)
    public MessageRouterConfiguration(@JsonProperty("queues") Map<String, QueueConfiguration> queues) {
        this.queues = queues;
        attributes = new HashMap<>();
        queues.forEach((queueAlias, queueConfiguration) -> {
            for (String attribute : queueConfiguration.getAttributes()) {
                attributes.computeIfAbsent(attribute, key -> new HashSet<>()).add(queueAlias);
            }
        });
    }

    @Nullable
    public QueueConfiguration getQueueByAlias(String queueAlias) {
        return queues.get(queueAlias);
    }

    public Set<String> getQueuesAliasByAttribute(String... types) {
        var list = Arrays.stream(types).map(type -> this.attributes.get(type)).collect(Collectors.toList());

        var iterator = list.iterator();

        Set<String> start = iterator.next();

        if (start == null) {
            return emptySet();
        }


        while (iterator.hasNext()) {
            Set<String> next = iterator.next();

            if (next == null) {
                return emptySet();
            }

            start = new HashSet<>(CollectionUtils.intersection(start, next));
        }

        return start;
    }

    public Set<String> getQueueAliasByMessageFilter(MessageFilter filter) {
        return queues
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().getFilters().stream().anyMatch(it -> checkFilter(it, filter)))
                .map(Entry::getKey).collect(Collectors.toSet());
    }

    private boolean checkFilter(RouterFilterConfiguration filterConfiguration, MessageFilter messageFilter) {
        var metadata = filterConfiguration.getMetadata();
        var session_alias = metadata.get(SESSION_ALIAS_KEY);
        var direction = metadata.get(DIRECTION_KEY);
        var messageType = metadata.get(MESSAGE_TYPE_KEY);

        if (session_alias != null && !session_alias.checkValue(messageFilter.getConnectionId().getSessionAlias())) {
            return false;
        }

        if (direction != null && !direction.checkValue((messageFilter.getDirection()))) {
            return false;
        }

        if (messageType != null && !messageType.checkValue((messageFilter.getMessageType()))) {
            return false;
        }

        var filters = filterConfiguration.getMessage();

        return messageFilter.getFieldsMap().entrySet().stream().allMatch(entry -> {
            FilterConfiguration fConfig = filters.get(entry.getKey());
            return fConfig == null || entry.getValue().getKindCase() == KindCase.SIMPLE_FILTER && fConfig.checkValue(entry.getValue().getSimpleFilter());
        });
    }

}

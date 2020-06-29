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
package com.exactpro.th2.common.message.configuration;

import com.exactpro.th2.configuration.FilterableConfiguration;
import com.exactpro.th2.infra.grpc.MessageFilter;
import com.exactpro.th2.infra.grpc.ValueFilter.KindCase;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;

public class MessageRouterConfiguration extends FilterableConfiguration {

    @JsonProperty
    private Map<String, QueueConfiguration> queues;

    @JsonProperty
    @JsonAlias({"tags", "labels"})
    private Map<String, Set<String>> attributes;

    @JsonProperty
    private Map<String, Set<String>> queueFilters;

    public Map<String, QueueConfiguration> getQueues() {
        return queues;
    }

    public void setQueues(Map<String, QueueConfiguration> queues) {
        this.queues = queues;
    }

    public Map<String, Set<String>> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Set<String>> attributes) {
        this.attributes = attributes;
    }

    public Map<String, Set<String>> getQueueFilters() {
        return queueFilters;
    }

    public void setQueueFilters(Map<String, Set<String>> queueFilters) {
        this.queueFilters = queueFilters;
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
        return filters
                .entrySet()
                .stream()
                .filter(entity -> checkFilter(entity.getValue(), filter))
                .map(entity -> queueFilters.get(entity.getKey()))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
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

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

import static java.util.Collections.emptySet;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.infra.grpc.MessageFilter;
import com.exactpro.th2.infra.grpc.ValueFilter.KindCase;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MessageRouterConfiguration {

    private static final String SESSION_ALIAS_KEY = "session_alias";
    private static final String DIRECTION_KEY = "direction";
    private static final String MESSAGE_TYPE_KEY = "message_type";

    @JsonProperty
    private Map<String, QueueConfiguration> queues;

    @JsonProperty
    private Map<String, Set<String>> attributes;

    @JsonProperty
    private Map<String, RouterFilterConfiguration> filters;

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

    public Map<String, RouterFilterConfiguration> getFilters() {
        return filters;
    }

    public void setFilters(Map<String, RouterFilterConfiguration> filters) {
        this.filters = filters;
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

    public Set<String> getQueueAliasByMesageFilter(MessageFilter filter) {
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

        if (session_alias != null && !checkValues(messageFilter.getConnectionId().getSessionAlias(), session_alias)) {
            return false;
        }

        if (direction != null && !checkValues(messageFilter.getDirection(), direction)) {
            return false;
        }

        if (messageType != null && !checkValues(messageFilter.getMessageType(), messageType)) {
            return false;
        }

        var filters = filterConfiguration.getMessage();

        return messageFilter.getFieldsMap().entrySet().stream().allMatch(entry -> {
            FilterConfiguration tmp = filters.get(entry.getKey());
            return tmp == null || entry.getValue().getKindCase() == KindCase.SIMPLE_FILTER && checkValues(entry.getValue().getSimpleFilter(), tmp);
        });
    }

    private boolean checkValues(String value1, FilterConfiguration filterConfiguration) {
        var value2 = filterConfiguration.getValue();
        switch (filterConfiguration.getOperation()) {
        case EQUAL:
            return value1.equals(value2);
        case NOT_EQUAL:
            return !value1.equals(value2);
        case EMPTY:
            return StringUtils.isEmpty(value1);
        case NOT_EMPTY:
            return StringUtils.isNotEmpty(value1);
        default:
            return false;
        }
    }

}

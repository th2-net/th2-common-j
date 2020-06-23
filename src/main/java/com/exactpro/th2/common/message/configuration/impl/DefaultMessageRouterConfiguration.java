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
package com.exactpro.th2.common.message.configuration.impl;

import static java.util.Collections.emptySet;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import com.exactpro.th2.common.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.common.message.impl.rabbitmq.configuration.impl.DefaultRabbitMQConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize
public class DefaultMessageRouterConfiguration implements MessageRouterConfiguration {

    @JsonProperty
    private DefaultRabbitMQConfiguration rabbit;
    @JsonProperty
    private Map<String, DefaultQueueConfiguration> queues;
    @JsonProperty
    private Map<String, Set<String>> types;
    @JsonProperty
    private Map<String, DefaultRouterFilterConfiguration> filters;
    @JsonProperty
    private Map<String, Set<String>> queueFilters;

    @Override
    public RabbitMQConfiguration getRabbitMQConfiguration() {
        return rabbit;
    }

    @Override
    public Set<String> getQueuesByTypes(String... types) {
        var list = Arrays.stream(types).map(type -> this.types.get(type)).collect(Collectors.toList());

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

    @Override
    public Set<String> getQueuesByTypesAndFilters(String[] types, String[] filters) {
        return null;
    }
}

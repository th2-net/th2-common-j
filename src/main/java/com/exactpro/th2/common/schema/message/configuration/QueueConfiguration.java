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

package com.exactpro.th2.common.schema.message.configuration;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueueConfiguration implements Configuration {

    /**
     * Routing key in RabbitMQ
     */
    @JsonProperty(required = true)
    private String name;

    /**
     * Queue name in RabbitMQ
     */
    @JsonProperty(required = true)
    private String queue;

    @JsonProperty(required = true)
    private String exchange;

    @JsonAlias({"labels", "tags"})
    @JsonProperty(required = true)
    private List<String> attributes = Collections.emptyList();

    @JsonProperty
    private List<MqRouterFilterConfiguration> filters = Collections.emptyList();

    @JsonProperty(value = "read", defaultValue = "true")
    private boolean isReadable = true;

    @JsonProperty(value = "write", defaultValue = "true")
    private boolean isWritable = true;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    public List<MqRouterFilterConfiguration> getFilters() {
        return filters;
    }

    public void setFilters(List<MqRouterFilterConfiguration> filters) {
        this.filters = filters;
    }

    public boolean isReadable() {
        return isReadable;
    }

    public void setReadable(boolean readable) {
        isReadable = readable;
    }

    public boolean isWritable() {
        return isWritable;
    }

    public void setWritable(boolean writable) {
        isWritable = writable;
    }
}

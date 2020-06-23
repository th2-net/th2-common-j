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

import java.util.Map;

import com.exactpro.th2.common.message.configuration.RouterFilterConfiguration;
import com.exactpro.th2.infra.grpc.MessageFilter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize
public class DefaultRouterFilterConfiguration implements RouterFilterConfiguration {

    @JsonProperty
    private Map<String, DefaultFilterConfiguration> metadata;
    @JsonProperty
    private Map<String, DefaultFilterConfiguration> message;

    @Override
    public Map<String, DefaultFilterConfiguration> getMetadata() {
        return metadata;
    }

    @Override
    public Map<String, DefaultFilterConfiguration> getMessage() {
        return message;
    }

    @Override
    public boolean inspect(MessageFilter messageFilter) {
        return false;
    }
}

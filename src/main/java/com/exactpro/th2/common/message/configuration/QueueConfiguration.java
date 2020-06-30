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

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

public class QueueConfiguration {

    @Getter
    @JsonProperty(required = true)
    private String name;

    @Getter
    @JsonProperty(required = true)
    private String exchange;

    @Getter
    @JsonAlias({"labels", "tags"})
    @JsonProperty(required = true)
    private String[] attributes;

    @Getter
    @JsonProperty
    private RouterFilterConfiguration[] filters;


}

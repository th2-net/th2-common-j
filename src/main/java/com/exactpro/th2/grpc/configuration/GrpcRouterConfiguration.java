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
package com.exactpro.th2.grpc.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.grpc.stub.AbstractStub;
import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
public class GrpcRouterConfiguration {

    @JsonProperty
    private Map<String, Map<Class<?>, String>> services;

    @JsonProperty
    private Map<String, Class<? extends AbstractStub>> serviceToStubMatch;
    @JsonProperty
    private Map<String, Set<String>> serviceToFiltersMatch;

    @JsonProperty
    private Map<String, GrpcConfiguration> servers;

    @JsonProperty
    private Map<String, GrpcRouterFilterConfiguration> filters;

    @JsonProperty
    private Map<String, String> filterToServerMatch;

    @JsonProperty
    private GrpcConfiguration serverConfiguration;

}

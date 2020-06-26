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

import java.util.Map;
import java.util.Set;

import com.exactpro.th2.common.message.configuration.RouterFilterConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;

public class GrpcRouterConfiguration {

    @JsonProperty
    private Map<String, GrpcConfiguration> servers;

    @JsonProperty
    private Map<Class<?>, Set<String>> classes;

    @JsonProperty
    private Map<Class<?>, StubConfiguration> stubs;

    @JsonProperty
    private Map<String, RouterFilterConfiguration> filters;

    @JsonProperty
    private Map<String, Set<String>> grpcFilters;

    @JsonProperty
    private GrpcConfiguration server;

    public Map<String, GrpcConfiguration> getServers() {
        return servers;
    }

    public void setServers(Map<String, GrpcConfiguration> servers) {
        this.servers = servers;
    }

    public Map<String, RouterFilterConfiguration> getFilters() {
        return filters;
    }

    public void setFilters(Map<String, RouterFilterConfiguration> filters) {
        this.filters = filters;
    }

    public Map<Class<?>, Set<String>> getClasses() {
        return classes;
    }

    public void setClasses(Map<Class<?>, Set<String>> classes) {
        this.classes = classes;
    }

    public Map<String, Set<String>> getGrpcFilters() {
        return grpcFilters;
    }

    public void setGrpcFilters(Map<String, Set<String>> grpcFilters) {
        this.grpcFilters = grpcFilters;
    }

    public Map<Class<?>, StubConfiguration> getStubs() {
        return stubs;
    }

    public void setStubs(Map<Class<?>, StubConfiguration> stubs) {
        this.stubs = stubs;
    }

    public GrpcConfiguration getServerConfiguration() {
        return server;
    }
}

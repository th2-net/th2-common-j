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

package com.exactpro.th2.common.schema.grpc.configuration;

import java.util.Map;

import com.exactpro.th2.common.schema.message.configuration.Configuration;
import com.exactpro.th2.common.schema.strategy.route.RoutingStrategy;
import com.fasterxml.jackson.annotation.JsonProperty;

public class GrpcServiceConfiguration implements Configuration {

    @JsonProperty(required = true)
    private RoutingStrategy<?> strategy;

    @JsonProperty(value = "service-class", required = true)
    private Class<?> serviceClass;

    @JsonProperty(required = true)
    private Map<String, GrpcEndpointConfiguration> endpoints;

    public RoutingStrategy<?> getStrategy() {
        return strategy;
    }

    public void setStrategy(RoutingStrategy<?> strategy) {
        this.strategy = strategy;
    }

    public Class<?> getServiceClass() {
        return serviceClass;
    }

    public void setServiceClass(Class<?> serviceClass) {
        this.serviceClass = serviceClass;
    }

    public Map<String, GrpcEndpointConfiguration> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(Map<String, GrpcEndpointConfiguration> endpoints) {
        this.endpoints = endpoints;
    }
}

/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.strategy.route.impl;

import com.exactpro.th2.common.schema.grpc.configuration.GrpcEndpointConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRawRobinStrategy;
import com.exactpro.th2.common.schema.strategy.route.RoutingStrategy;
import com.exactpro.th2.common.schema.strategy.route.StrategyName;
import com.google.protobuf.Message;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@StrategyName("robin")
public class RobinRoutingStrategy implements RoutingStrategy<GrpcRawRobinStrategy> {

    private GrpcRawRobinStrategy configuration = new GrpcRawRobinStrategy();
    private final AtomicInteger index = new AtomicInteger(0);

    @Override
    public Class<? extends GrpcRawRobinStrategy> getConfigurationClass() {
        return GrpcRawRobinStrategy.class;
    }

    @Override
    public void init(GrpcRawRobinStrategy configuration) {
        if (configuration != null) {
            this.configuration = configuration;
        }
    }

    @Override
    public String getName() {
        return "robin";
    }

    @Override
    public GrpcRawRobinStrategy getConfiguration() {
        return configuration;
    }

    @Override
    public String getEndpoint(Message message) {
        return configuration.getEndpoints().get(index.getAndUpdate(i -> i + 1 >= configuration.getEndpoints().size() ? 0 : i + 1));
    }

    @Override
    public String getEndpoint(Message message, Map<String, GrpcEndpointConfiguration> endPoints, String... attrs) {
        return getEndpoint(message); // TODO attributes filter
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RobinRoutingStrategy that = (RobinRoutingStrategy) o;
        return Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configuration);
    }
}

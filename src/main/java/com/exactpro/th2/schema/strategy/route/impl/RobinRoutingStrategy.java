/*****************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/

package com.exactpro.th2.schema.strategy.route.impl;

import com.exactpro.th2.schema.grpc.configuration.GrpcRawRobinStrategy;
import com.exactpro.th2.schema.strategy.route.RoutingStrategy;
import com.exactpro.th2.schema.strategy.route.StrategyName;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@StrategyName("robin")
public class RobinRoutingStrategy implements RoutingStrategy<GrpcRawRobinStrategy> {

    private List<String> endpoints;
    private AtomicInteger index = new AtomicInteger(0);


    @Override
    public Class<? extends GrpcRawRobinStrategy> getConfigurationClass() {
        return GrpcRawRobinStrategy.class;
    }

    @Override
    public void init(GrpcRawRobinStrategy configuration) {
        this.endpoints = new ArrayList<>(configuration.getEndpoints());
    }

    @Override
    public String getEndpoint(Message message) {
        return endpoints.get(index.getAndUpdate(i -> i + 1 >= endpoints.size() ? 0 : i + 1));
    }

}

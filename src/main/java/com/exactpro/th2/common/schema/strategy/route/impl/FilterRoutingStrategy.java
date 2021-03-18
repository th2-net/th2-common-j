/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.filter.strategy.FilterStrategy;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRawFilterStrategy;
import com.exactpro.th2.common.schema.strategy.route.RoutingStrategy;
import com.exactpro.th2.common.schema.strategy.route.StrategyName;
import com.google.protobuf.Message;

import java.util.HashSet;
import java.util.Set;

@StrategyName("filter")
public class FilterRoutingStrategy implements RoutingStrategy<GrpcRawFilterStrategy> {

    private GrpcRawFilterStrategy grpcConfiguration;

    private final FilterStrategy<Message> filterStrategy = FilterStrategy.DEFAULT_FILTER_STRATEGY;


    @Override
    public Class<? extends GrpcRawFilterStrategy> getConfigurationClass() {
        return GrpcRawFilterStrategy.class;
    }

    @Override
    public void init(GrpcRawFilterStrategy configuration) {
        this.grpcConfiguration = configuration;
    }

    @Override
    public String getEndpoint(Message message) {
        var endpoint = filter(message);

        if (endpoint.size() != 1) {
            throw new IllegalStateException("Wrong size of endpoints for send. Should be equal to 1");
        }

        return endpoint.iterator().next();
    }


    private Set<String> filter(Message message) {

        var endpoints = new HashSet<String>();

        for (var fieldsFilter : grpcConfiguration.getFilters()) {

            if (fieldsFilter.getMessage().isEmpty() && fieldsFilter.getMetadata().isEmpty()
                    || filterStrategy.verify(message, fieldsFilter)) {
                endpoints.add(fieldsFilter.getEndpoint());
            }
        }

        return endpoints;
    }

}

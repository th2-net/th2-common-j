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
package com.exactpro.th2.schema.strategy.route.impl;

import com.exactpro.th2.schema.filter.Filter;
import com.exactpro.th2.schema.filter.factory.FilterFactory;
import com.exactpro.th2.schema.filter.factory.impl.DefaultFilterFactory;
import com.exactpro.th2.schema.grpc.configuration.GrpcRawStrategy;
import com.exactpro.th2.schema.strategy.route.RoutingStrategy;
import com.exactpro.th2.schema.strategy.route.StrategyName;
import com.google.protobuf.Message;

@StrategyName("filter")
public class FilterStrategy implements RoutingStrategy<GrpcRawStrategy> {

    private Filter filter;

    private FilterFactory factory = new DefaultFilterFactory();


    @Override
    public Class<? extends GrpcRawStrategy> getConfigurationClass() {
        return GrpcRawStrategy.class;
    }

    @Override
    public void init(GrpcRawStrategy configuration) {
        this.filter = factory.createFilter(configuration);
    }

    @Override
    public String getEndpoint(Message message) {
        return filter.check(message);
    }

}

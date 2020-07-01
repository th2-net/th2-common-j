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
package com.exactpro.th2.common.strategy.routeStrategy.json;

import java.io.IOException;

import com.exactpro.th2.common.strategy.routeStrategy.RoutingStrategy;
import com.exactpro.th2.common.strategy.routeStrategy.RoutingStrategyFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class JsonDeserializerRoutingStategy extends StdDeserializer<RoutingStrategy> {

    private RoutingStrategyFactory strategyFactory;
    private ObjectMapper mapper;

    public JsonDeserializerRoutingStategy() {
        super(RoutingStrategy.class);

        strategyFactory = new RoutingStrategyFactory();

        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public RoutingStrategy<?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {

        JsonNode node = p.getCodec().readTree(p);

        if (!node.has("name")) {
            throw new RuntimeException("Can not find name for the strategy");
        }

        if (!node.get("name").isTextual()) {
            throw new RuntimeException("Field 'name' is not text in strategy block");
        }

        String strategyName = node.get("name").asText();
        var strategy = strategyFactory.getStrategy(strategyName);

        if (strategy == null) {
            throw new IllegalStateException("Can not find strategy with name: " + strategyName);
        }

        Class<?> configurationClass;
        try {
            configurationClass = strategy.getConfigurationClass();
            if (configurationClass == null) {
                throw new RuntimeException("Can not init strategy, because configuration class is null");
            }
        } catch (Exception e) {
            throw new RuntimeException("Can not get configuration class from strategy", e);
        }

        var strategyConfiguration = mapper.readerFor(configurationClass).readValue(node);

        try {
            strategy.init(strategyConfiguration);
            return strategy;
        } catch (Exception e) {
            throw new RuntimeException("Can not init strategy with class: " + strategy.getClass(), e);
        }
    }
}

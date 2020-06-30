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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import com.exactpro.th2.common.strategy.routeStrategy.RoutingStrategy;
import com.exactpro.th2.common.strategy.routeStrategy.StrategyName;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class JsonDeserializerRoutingStategy extends StdDeserializer<RoutingStrategy> {

    private Map<String, Class<? extends RoutingStrategy>> strategies = new HashMap<>();
    private Logger logger = LoggerFactory.getLogger(this.getClass() + "@" + this.hashCode());

    private ObjectMapper mapper = new ObjectMapper();

    //FIXME: Which arguments we need for this
    public JsonDeserializerRoutingStategy() {
        super(RoutingStrategy.class);

        ServiceLoader.load(RoutingStrategy.class).stream().forEach(provider -> {

            Class<? extends RoutingStrategy> cls = provider.type();

            StrategyName nameAnnotation = cls.getAnnotation(StrategyName.class);
            String name = nameAnnotation != null && StringUtils.isNotEmpty(nameAnnotation.value()) ? nameAnnotation.value() : cls.getSimpleName();

            try {
                Constructor constructor = cls.getConstructor();
                if (constructor.canAccess(this)) {
                    Class<? extends RoutingStrategy> prev = strategies.put(name, cls);

                    if (prev != null) {
                        throw new IllegalStateException("Strategies names were duplicated with value: '" + name + "'. Classes: '" + cls + "' and '" + prev + "'.");
                    }
                } else {
                    logger.warn("Strategy with name '{}' and class '{}' not added to possible strategy, because class have not access to default constructor", name, cls);
                }
            } catch (NoSuchMethodException e) {
                logger.warn("Strategy with name '{}' and class '{}' not added to possible strategy, because class have not default constructor", name, cls, e);
            }
        });
    }

    @Override
    public RoutingStrategy<?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {

        JsonNode node = p.getCodec().readTree(p);

        if (!node.has("name")) {
            throw new IllegalStateException("Can not find name for the strategy");
        }

        if (!node.get("name").isTextual()) {
            throw new IllegalStateException("Field 'name' is not text in strategy block");
        }

        String strategyName = node.get("name").asText();
        var strategyClass = strategies.get(strategyName);

        if (strategyClass == null) {
            throw new IllegalStateException("Can not find strategy with name: " + strategyName);
        }

        try {
            var strategy = strategyClass.getConstructor().newInstance();
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
                throw new RuntimeException("Can not init strategy with class: " + strategyClass, e);
            }

        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException("Can not create strategies instance from default constructor", e);
        }
    }
}

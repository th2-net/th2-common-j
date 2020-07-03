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
package com.exactpro.th2.schema.strategy.route;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoutingStrategyFactory {

    private Logger logger = LoggerFactory.getLogger(this.getClass() + "@" + this.hashCode());
    private Map<String, Class<? extends RoutingStrategy>> strategies = new HashMap<>();

    public RoutingStrategyFactory() {
        ServiceLoader.load(RoutingStrategy.class).stream().forEach(provider -> {

            Class<? extends RoutingStrategy> cls = provider.type();

            StrategyName nameAnnotation = cls.getAnnotation(StrategyName.class);
            String name = nameAnnotation != null && StringUtils.isNotEmpty(nameAnnotation.value()) ? nameAnnotation.value() : cls.getSimpleName();

            try {
                Constructor constructor = cls.getConstructor();
                if (constructor.canAccess(null)) {
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

    public RoutingStrategy getStrategy(String alias) {
        var strategyClass = strategies.get(alias);
        if (strategyClass == null) {
            return null;
        }

        try {
            return strategyClass.getConstructor().newInstance();
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException("Can not create strategies instance from default constructor", e);
        }
    }

}

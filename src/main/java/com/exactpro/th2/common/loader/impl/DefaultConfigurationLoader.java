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
package com.exactpro.th2.common.loader.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;

import com.exactpro.th2.common.loader.ConfigurationLoader;

public class DefaultConfigurationLoader implements ConfigurationLoader {

    private DefaultConfigurationLoader() {}

    public static DefaultConfigurationLoader INSTANSE = new DefaultConfigurationLoader();

    public <T> Class<? extends T> load(Class<T> _class, Class<?>... types) {
        var stream = ServiceLoader.load(_class).stream();
        if (types.length > 0) {
            stream = stream.filter(provider -> haveGenerics(provider.type(), types));
        }

        List<Provider<T>> list = stream.collect(Collectors.toList());

        switch (list.size()) {
        case 1:
            return list.get(0).type();
        case 2:
            if (list.get(0).type().getSimpleName().startsWith("Default")) {
                return list.get(1).type();
            } else if (list.get(1).type().getSimpleName().startsWith("Default")) {
                list.get(0).type();
            } else {
                throw new IllegalStateException();
            }
        default:
            if (list.size() < 1) {
                throw new IllegalStateException("Can not find realization for class: " + _class);
            } else {
                throw new IllegalStateException("Too mush realization for class: " + _class);
            }
        }
    }

    @Override
    public <T> T createInstance(Class<T> _class, Class<?>... types) throws IllegalStateException {
        Class<? extends T> cls = load(_class, types);
        try {
            Constructor<? extends T> defaultConstructor = cls.getConstructor();
            return defaultConstructor.newInstance();
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Can not find default constructor for class: " + cls, e);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalStateException("Can not execute default constructor for class: " + cls, e);
        }
    }

    private boolean haveGenerics(Class<?> cls, Class<?>... types) {
        Type[] genericInterfaces = cls.getGenericInterfaces();
        if (genericInterfaces.length < 1) {
            Type genericSuperclass = cls.getGenericSuperclass();
            if (genericSuperclass == null) {
                Class<?> superclass = cls.getSuperclass();
                return superclass != null && haveGenerics(superclass, types);
            }

            return Arrays.deepEquals(((ParameterizedType)genericSuperclass).getActualTypeArguments(), types);
        }

        for (Type genericInterface : genericInterfaces) {
            if (genericInterface instanceof ParameterizedType) {
                return Arrays.deepEquals(((ParameterizedType)genericInterface).getActualTypeArguments(), types);
            }
        }
        return false;
    }
}

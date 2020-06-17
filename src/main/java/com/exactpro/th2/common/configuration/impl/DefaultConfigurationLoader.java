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
package com.exactpro.th2.common.configuration.impl;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;

import com.exactpro.th2.ConfigurationUtils;
import com.exactpro.th2.common.configuration.IConfigurationLoader;

public class DefaultConfigurationLoader implements IConfigurationLoader {

    private DefaultConfigurationLoader() {
    }

    public static DefaultConfigurationLoader INSTANSE = new DefaultConfigurationLoader();

    @Override
    public <T> Class<? extends T> load(Class<T> _class) throws IllegalStateException {
        List<Provider<T>> list = ServiceLoader.load(_class).stream().collect(Collectors.toList());

        switch (list.size()) {
        case 1:
            return list.get(0).type();
        case 2:
            return (list.get(0).type().getSimpleName().startsWith("Default") ? list.get(1) : list.get(0)).type();
        default:
            if (list.size() < 1) {
                throw new IllegalStateException("Can not find realization for class: " + _class);
            } else {
                throw new IllegalStateException("Too mush realization for class: " + _class);
            }
        }
    }

    @Override
    public <T> T load(Class<T> _class, InputStream inputStream) throws IllegalStateException, IOException {
        Class<? extends T> cls = load(_class);

        try {
            Constructor<? extends T> constructor = cls.getConstructor();

            try {
                Method loadMethod = cls.getMethod("load", InputStream.class);

                return ConfigurationUtils.safeLoad(
                        inputStream1 -> {
                            try {
                                return (T)loadMethod.invoke(null, inputStream1);
                            } catch (IllegalAccessException | InvocationTargetException e) {
                                throw new IllegalStateException("Can not invoke load method", e);
                            }
                        },
                        () -> {
                            try {
                                return constructor.newInstance();
                            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                                throw new IllegalStateException("Can not load from input stream. Can not invoke default constructor", e);
                            }
                        },
                        inputStream);

            } catch (NoSuchMethodException e) {
                try {
                    return constructor.newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException ex) {
                    throw new IllegalStateException("Can not find method with name 'load' and can not invoke default constructor", ex);
                }
            }
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Can not find default constructor for class: " + _class);
        }
    }

}

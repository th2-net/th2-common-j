/******************************************************************************
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
 ******************************************************************************/
package com.exactpro.th2;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationUtils {

    private final static Logger LOGGER = LoggerFactory.getLogger(ConfigurationUtils.class);

    /**
     * Tries to execute load function. If success returns its result otherwise execute create default function and returns its result.
     * @param loadable Function loads T from {@link InputStream}
     * @param createDefault Function creates default value of type T
     * @param filePath Path to file
     * @param <T>
     * @return Object of type T which safe loaded
     */
    public static <T> T safeLoad(Loadable<T> loadable, Supplier<T> createDefault, String filePath) {
        if (StringUtils.isNotBlank(filePath)) {
            Path path = Paths.get(filePath);
            if (Files.exists(path)) {
                try (InputStream inputStream = Files.newInputStream(path)) {
                    return loadable.load(inputStream);
                } catch (IOException e) {
                    LOGGER.warn("Loading from file {} failure", filePath, e);
                }
            } else {
                LOGGER.warn("Path {} to file isn't exist", filePath);
            }
        } else {
            LOGGER.warn("Path to file is blank");
        }

        return createDefault.get();
    }

    @FunctionalInterface
    public static interface Loadable<T> {
        public T load(InputStream inputStream) throws IOException;
    }
}

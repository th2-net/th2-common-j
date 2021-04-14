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

package com.exactpro.th2.common.schema.configuration

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.text.StringSubstitutor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Supplier

class ConfigurationManager(private val configurationPath: Map<Class<*>, Path>) {

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(ConfigurationManager::class.java)
    }

    private val configurations: MutableMap<Class<*>, Any?> = ConcurrentHashMap()


    fun <T> loadConfiguration(
        configPath: Path,
        configClass: Class<T>,
        stringSubstitutor: StringSubstitutor,
        objectMapper: ObjectMapper
    ): T {
        try {
            val sourceContent = String(Files.readAllBytes(configPath))
            LOGGER.info("Configuration path {} source content {}", configPath, sourceContent)
            val content: String = stringSubstitutor.replace(sourceContent)
            return objectMapper.readerFor(configClass).readValue(content)
        } catch (e: IOException) {
            throw IllegalStateException("Cannot read ${configClass.name} configuration from path '${configPath}'", e)
        }
    }

    fun <T> loadConfiguration(
        configClass: Class<T>,
        stringSubstitutor: StringSubstitutor,
        objectMapper: ObjectMapper
    ): T? = configurationPath[configClass]?.let {
        loadConfiguration(it, configClass, stringSubstitutor, objectMapper)
    }

    fun <T> getConfigurationOrLoad(
        configPath: Path,
        configClass: Class<T>,
        stringSubstitutor: StringSubstitutor,
        objectMapperSupplier: Supplier<ObjectMapper>
    ): T {
        return configurations.computeIfAbsent(configClass) {
            loadConfiguration(configPath, configClass, stringSubstitutor, objectMapperSupplier.get())
        } as T? ?: throw IllegalStateException("Can not load configuration for class ${configClass.name}")
    }

    fun <T> getConfigurationOrLoad(
        configClass: Class<T>,
        stringSubstitutor: StringSubstitutor,
        objectMapperSupplier: Supplier<ObjectMapper>
    ): T? {
        return configurations.computeIfAbsent(configClass) {
            loadConfiguration(
                configClass,
                stringSubstitutor,
                objectMapperSupplier.get()
            )
        } as T?
    }

    fun <T> getConfiguration(configClass: Class<T>): T? = configurations[configClass] as T?

}
/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.ConfigurationProvider
import com.exactpro.th2.common.ModuleFactory
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import org.apache.commons.text.StringSubstitutor
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class ConfigurationManager(private val configurationPath: Map<Class<*>, Path>) {
    private val cache: MutableMap<Class<*>, Any?> = ConcurrentHashMap()
    private val modulesFactoryMapping: MutableMap<Class<*>, ModuleFactory> = ConcurrentHashMap()
    private val configurationFactoryMapping: MutableMap<Class<*>, ModuleFactory> = ConcurrentHashMap()

    init {
        ServiceLoader.load(ModuleFactory::class.java).forEach { moduleFactory ->
            moduleFactory.configurationClasses.forEach {
                configurationFactoryMapping[it] = moduleFactory
            }
            moduleFactory.moduleClasses.forEach {
                modulesFactoryMapping[it] = moduleFactory
            }
        }
    }

    fun <T> loadConfiguration(
        objectMapper: ObjectMapper,
        stringSubstitutor: StringSubstitutor,
        configClass: Class<T>,
        configPath: Path,
        optional: Boolean
    ): T {
        try {
            if (optional && !(Files.exists(configPath) && Files.size(configPath) > 0)) {
                LOGGER.warn { "Can not read configuration for ${configClass.name}. Use default configuration" }
                return configClass.getDeclaredConstructor().newInstance()
            }

            val sourceContent = String(Files.readAllBytes(configPath))
            LOGGER.info { "Configuration path $configClass source content $sourceContent" }
            val content: String = stringSubstitutor.replace(sourceContent)
            return objectMapper.readerFor(configClass).readValue(content)
        } catch (e: IOException) {
            throw IllegalStateException("Cannot read ${configClass.name} configuration from path '${configPath}'", e)
        }
    }

    fun <T> getModuleWithConfigurationProvider(
        moduleClass: Class<T>,
        configurationProvider: ConfigurationProvider
    ): T {
        val moduleFactory: ModuleFactory? = modulesFactoryMapping[moduleClass]
        checkNotNull(moduleFactory) {
            LOGGER.error { "Mapping does not contain module factory for $moduleClass" }
            "Mapping does not contain module factory for $moduleClass"
        }
        val module: T = moduleFactory.loadModule(configurationProvider, moduleClass) as T
        checkNotNull(module) {
            LOGGER.error { "Returned null instead of instance of class $moduleClass" }
            "Returned null instead of instance of class $moduleClass"
        }
        return module
    }

    fun <T : Configuration> getConfigurationWithConfigurationProvider(
        configClass: Class<T>,
        configurationProvider: ConfigurationProvider
    ): T {
        return cache.computeIfAbsent(configClass) {
            checkNotNull(configurationFactoryMapping[configClass]) {
            "Unknown class $configClass"
        }.loadConfiguration(configurationProvider, configClass)
        } as T
    }

    fun <T> getConfigurationOrLoad(
        objectMapper: ObjectMapper,
        stringSubstitutor: StringSubstitutor,
        configClass: Class<T>,
        optional: Boolean
    ): T {
        return cache.computeIfAbsent(configClass) {
            checkNotNull(configurationPath[configClass]) {
                "Unknown class $configClass"
            }.let {
                loadConfiguration(objectMapper, stringSubstitutor, configClass, it, optional)
            }
        } as T
    }

    companion object {
        val LOGGER = KotlinLogging.logger {}
    }
}
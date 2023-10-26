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

import mu.KotlinLogging
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

class ConfigurationManager(
    private val configurationProvider: IConfigurationProvider,
    private val configurationPath: Map<Class<*>, String>
) {
    private val configurations: MutableMap<Class<*>, Any?> = ConcurrentHashMap()

    operator fun get(clazz: Class<*>): String? = configurationPath[clazz]

    fun <T : Any> loadConfiguration(
        configClass: Class<T>,
        configAlias: String,
        optional: Boolean
    ): T {
        try {
            return configurationProvider.load(configAlias, configClass) {
                if (!optional) {
                    throw IllegalStateException("The '$configAlias' is required")
                }
                configClass.getDeclaredConstructor().newInstance()
            }
        } catch (e: IOException) {
            throw IllegalStateException("Cannot read ${configClass.name} configuration for config alias '$configAlias'", e)
        }
    }

    fun <T : Any> getConfigurationOrLoad(
        configClass: Class<T>,
        optional: Boolean
    ): T {
        return configurations.computeIfAbsent(configClass) {
            checkNotNull(configurationPath[configClass]) {
                "Unknown class $configClass"
            }.let {
                loadConfiguration(configClass, it, optional)
            }
        }.let(configClass::cast)
    }

    companion object {
        val LOGGER = KotlinLogging.logger {}
    }
}
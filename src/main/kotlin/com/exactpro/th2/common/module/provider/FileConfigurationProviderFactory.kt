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

package com.exactpro.th2.common.module.provider

import com.exactpro.th2.common.ConfigurationProvider
import com.exactpro.th2.common.ConfigurationProviderFactory
import com.exactpro.th2.common.schema.configuration.ConfigurationProviderConfig
import com.exactpro.th2.common.schema.strategy.route.json.RoutingStrategyModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

open class FileConfigurationProviderFactory : ConfigurationProviderFactory {

    private val objectMapper: ObjectMapper = ObjectMapper()

    init {
        objectMapper.registerKotlinModule()
        objectMapper.registerModules(
            RoutingStrategyModule(objectMapper),
            JavaTimeModule()
        )
    }

    override val type: Class<out ConfigurationProvider>
        get() = FileConfigurationProvider::class.java

    override fun create(config: ConfigurationProviderConfig): ConfigurationProvider {
        if (config is FileConfigurationProviderConfig) {
            return FileConfigurationProvider(
                objectMapper,
                config.fileProviderPath,
                config.fileProviderExtension
            )
        } else {
            throw IllegalArgumentException()
        }
    }

    override fun settings(): ConfigurationProviderConfig {
        return FileConfigurationProviderConfig()
    }
}
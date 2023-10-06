/*
 *  Copyright 2023 Exactpro (Exactpro Systems Limited)
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.common.schema.configuration.impl

import com.exactpro.th2.common.schema.configuration.IConfigurationProvider
import com.exactpro.th2.common.schema.strategy.route.json.RoutingStrategyModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import mu.KotlinLogging
import org.apache.commons.text.StringSubstitutor
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.exists
import kotlin.io.path.isDirectory

class JsonConfigurationProvider(
    private val baseDir: Path
) : IConfigurationProvider {

    init {
        require(baseDir.isDirectory()) {
            "The '$baseDir' base directory doesn't exist or isn't directory"
        }
    }

    private val cache = ConcurrentHashMap<String, Any>()

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any> load(configClass: Class<T>, alias: String, default: () -> T): T =
        cache.computeIfAbsent(alias) {
            val configPath = baseDir.resolve("${alias}.${EXTENSION}")
            if (!configPath.exists()) {
                K_LOGGER.debug { "'$configPath' file related to the '$alias' config alias doesn't exist" }
                return@computeIfAbsent default()
            }

            val sourceContent = String(Files.readAllBytes(configPath))
            K_LOGGER.info { "'$configPath' file related to the '$alias' config alias has source content $sourceContent" }
            val content = SUBSTITUTOR.get().replace(sourceContent)
            requireNotNull(MAPPER.readValue(content, configClass)) {
                "Parsed format of the '$alias' config alias content can't be null"
            }
        }.also { value ->
            check(configClass.isInstance(value)) {
                "Stored configuration instance of $alias config alias mismatches, " +
                        "expected: ${configClass.canonicalName}, actual: ${value::class.java.canonicalName}"
            }
        } as T

    companion object {
        private const val EXTENSION = "json"

        private val K_LOGGER = KotlinLogging.logger {}
        private val SUBSTITUTOR: ThreadLocal<StringSubstitutor> = object : ThreadLocal<StringSubstitutor>() {
            override fun initialValue(): StringSubstitutor = StringSubstitutor(System.getenv())
        }

        @JvmField
        val MAPPER = ObjectMapper().apply {
            registerModules(
                KotlinModule.Builder()
                    .withReflectionCacheSize(512)
                    .configure(KotlinFeature.NullToEmptyCollection, false)
                    .configure(KotlinFeature.NullToEmptyMap, false)
                    .configure(KotlinFeature.NullIsSameAsDefault, false)
                    .configure(KotlinFeature.SingletonSupport, false)
                    .configure(KotlinFeature.StrictNullChecks, false)
                    .build(),
                RoutingStrategyModule(this),
                JavaTimeModule()
            )
        }
    }
}
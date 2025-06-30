/*
 *  Copyright 2023-2025 Exactpro (Exactpro Systems Limited)
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
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.commons.text.StringSubstitutor
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function
import java.util.function.Supplier
import kotlin.io.path.exists


class JsonConfigurationProvider private constructor(
    internal val baseDir: Path,
    internal val customPaths: Map<String, Path> = emptyMap(),
) : IConfigurationProvider {

    private val cache = ConcurrentHashMap<String, Any>()

    private fun getPathFor(alias: String): Path = customPaths[alias] ?: baseDir.resolve("${alias}.${EXTENSION}")

    override fun <T : Any> load(alias: String, configClass: Class<T>, default: Supplier<T>?): T =
        requireNotNull(cache.compute(alias) { _, value ->
            when {
                value == null -> loadFromFile(alias, default) { MAPPER.readValue(it, configClass) }
                !configClass.isInstance(value) -> loadFromFile(alias, default) {
                    MAPPER.readValue(it, configClass)
                }.also {
                    K_LOGGER.info { "Parsed value for '$alias' alias has been updated from: ${value::class.java} to ${it::class.java}" }
                }
                else -> value
            }
        }).also { value ->
            check(configClass.isInstance(value)) {
                "Stored configuration instance of $alias config alias mismatches, " +
                        "expected: ${configClass.canonicalName}, actual: ${value::class.java.canonicalName}"
            }
        }.let(configClass::cast)

    override fun <T : Any> load(alias: String, parser: Function<InputStream, T>, default: Supplier<T>?): T =
        loadFromFile(alias, default, parser).apply {
            cache.put(alias, this)?.let {
                K_LOGGER.info { "Parsed value for '$alias' alias has been updated from: ${this@JsonConfigurationProvider::class.java} to ${it::class.java}" }
            }
        }

    private fun <T : Any> loadFromFile(alias: String, default: Supplier<T>?, parser: Function<InputStream, T>): T {
        val configPath = this.getPathFor(alias)
        if (!configPath.exists()) {
            K_LOGGER.warn { "'$configPath' file related to the '$alias' config alias doesn't exist" }
            return default?.get()
                ?: error("Configuration loading failure, '$configPath' file related to the '$alias' config alias doesn't exist")
        }
        if (Files.size(configPath) == 0L) {
            K_LOGGER.warn { "'$configPath' file related to the '$alias' config alias has 0 size" }
            return default?.get()
                ?: error("Configuration loading failure, '$configPath' file related to the '$alias' config alias has 0 size")
        }

        val sourceContent = String(Files.readAllBytes(configPath))
        K_LOGGER.info { "'$configPath' file related to the '$alias' config alias has source content $sourceContent" }
        val content = SUBSTITUTOR.get().replace(sourceContent)
        return requireNotNull(parser.apply(content.byteInputStream())) {
            "Parsed format of config content can't be null, alias: '$alias'"
        }
    }

    companion object {
        private const val EXTENSION = "json"

        private val K_LOGGER = KotlinLogging.logger {}

        @Suppress("SpellCheckingInspection")
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
                    .configure(KotlinFeature.SingletonSupport, true)
                    .configure(KotlinFeature.StrictNullChecks, false)
                    .build(),
                RoutingStrategyModule(this),
                JavaTimeModule()
            )
        }

        @JvmStatic
        @JvmOverloads
        fun create(
            baseDir: Path,
            customPaths: Map<String, Path> = emptyMap()
        ): JsonConfigurationProvider = JsonConfigurationProvider(baseDir, customPaths)
    }
}
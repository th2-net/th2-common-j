/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.schema.factory

import com.exactpro.th2.common.metrics.PrometheusConfiguration
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration
import com.exactpro.th2.common.schema.configuration.impl.JsonConfigurationProvider
import com.exactpro.th2.common.schema.cradle.CradleConfidentialConfiguration
import com.exactpro.th2.common.schema.cradle.CradleNonConfidentialConfiguration
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.dictionary.DictionaryType.INCOMING
import com.exactpro.th2.common.schema.dictionary.DictionaryType.MAIN
import com.exactpro.th2.common.schema.dictionary.DictionaryType.OUTGOING
import com.exactpro.th2.common.schema.factory.CommonFactory.CONFIG_DEFAULT_PATH
import com.exactpro.th2.common.schema.factory.CommonFactory.TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY
import com.exactpro.th2.common.schema.factory.CommonFactoryTest.Companion.DictionaryHelper.ALIAS
import com.exactpro.th2.common.schema.factory.CommonFactoryTest.Companion.DictionaryHelper.Companion.assertDictionary
import com.exactpro.th2.common.schema.factory.CommonFactoryTest.Companion.DictionaryHelper.OLD
import com.exactpro.th2.common.schema.factory.CommonFactoryTest.Companion.DictionaryHelper.TYPE
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.exactpro.th2.common.schema.util.ArchiveUtils
import org.apache.commons.io.file.PathUtils.deleteDirectory
import org.apache.commons.lang3.RandomStringUtils
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.samePropertyValuesAs
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junitpioneer.jupiter.ClearSystemProperty
import org.junitpioneer.jupiter.SetSystemProperty
import java.nio.file.Path
import java.util.Locale
import kotlin.io.path.absolutePathString
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.writeBytes
import kotlin.reflect.cast
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull


@Suppress("DEPRECATION", "removal")
class CommonFactoryTest {

    @BeforeEach
    fun beforeEach() {
        if (TEMP_DIR.toPath().exists()) {
            deleteDirectory(TEMP_DIR.toPath())
        }
        CMD_ARG_CFG_DIR_PATH.createDirectories()
        SYSTEM_PROPERTY_CFG_DIR_PATH.createDirectories()
        CUSTOM_DIR_PATH.createDirectories()
    }

    @Nested
    @ClearSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY)
    inner class CreateByDefaultConstructor {
        @Test
        fun `test default`() {
            CommonFactory().use { commonFactory ->
                assertThrowsDictionaryDir(commonFactory)
                assertThrowsConfigs(commonFactory)
            }
        }

        @Test
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CFG_DIR)
        fun `test with system property`() {
            CommonFactory().use { commonFactory ->
                assertDictionaryDirs(commonFactory, SYSTEM_PROPERTY_CFG_DIR_PATH)
                assertConfigs(commonFactory, SYSTEM_PROPERTY_CFG_DIR_PATH)
            }
        }
    }

    @Nested
    @ClearSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY)
    inner class CreateBySettingsConstructor {
        @Test
        fun `test default`() {
            CommonFactory(FactorySettings()).use { commonFactory ->
                assertThrowsDictionaryDir(commonFactory)
                assertThrowsConfigs(commonFactory)
            }
        }

        @ParameterizedTest
        @EnumSource(value = DictionaryHelper::class)
        fun `test custom dictionary path`(helper: DictionaryHelper) {
            CommonFactory(FactorySettings().apply {
                helper.setOption(this, CUSTOM_DIR_PATH)
            }).use { commonFactory ->
                val content = helper.writeDictionaryByCustomPath(CUSTOM_DIR_PATH, DICTIONARY_NAME)
                assertDictionary(commonFactory, content)

                assertThrowsConfigs(commonFactory)
            }
        }

        @ParameterizedTest
        @EnumSource(ConfigHelper::class)
        fun `test custom path for config`(helper: ConfigHelper) {
            val configPath = CUSTOM_DIR_PATH.resolve(helper.alias)
            CommonFactory(FactorySettings().apply {
                helper.setOption(this, configPath)
            }).use { commonFactory ->
                assertThrowsDictionaryDir(commonFactory)

                val cfgBean = helper.writeConfigByCustomPath(configPath)
                assertThat(helper.loadConfig(commonFactory), samePropertyValuesAs(cfgBean))
            }
        }

        @Test
        fun `test with custom config path`() {
            CommonFactory(FactorySettings().apply {
                baseConfigDir = CMD_ARG_CFG_DIR_PATH
            }).use { commonFactory ->
                assertDictionaryDirs(commonFactory, CMD_ARG_CFG_DIR_PATH)
                assertConfigs(commonFactory, CMD_ARG_CFG_DIR_PATH)
            }
        }

        @ParameterizedTest
        @EnumSource(value = DictionaryHelper::class)
        fun `test with custom config path and custom dictionary path`(helper: DictionaryHelper) {
            val settings = FactorySettings().apply {
                baseConfigDir = CMD_ARG_CFG_DIR_PATH
                helper.setOption(this, CMD_ARG_CFG_DIR_PATH)
            }
            CommonFactory(settings).use { commonFactory ->
                val content = helper.writeDictionaryByCustomPath(CMD_ARG_CFG_DIR_PATH, DICTIONARY_NAME)
                assertDictionary(commonFactory, content)

                assertConfigs(commonFactory, CMD_ARG_CFG_DIR_PATH)
            }
        }

        @ParameterizedTest
        @EnumSource(ConfigHelper::class)
        fun `test with custom config path and custom path for config`(helper: ConfigHelper) {
            val configPath = CUSTOM_DIR_PATH.resolve(helper.alias)
            CommonFactory(FactorySettings().apply {
                baseConfigDir = CMD_ARG_CFG_DIR_PATH
                helper.setOption(this, configPath)
            }).use { commonFactory ->
                assertDictionaryDirs(commonFactory, CMD_ARG_CFG_DIR_PATH)

                val cfgBean = helper.writeConfigByCustomPath(configPath)
                assertThat(helper.loadConfig(commonFactory), samePropertyValuesAs(cfgBean))
            }
        }

        @ParameterizedTest
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CFG_DIR)
        @EnumSource(DictionaryHelper::class)
        fun `test with system property`(helper: DictionaryHelper) {
            CommonFactory(FactorySettings()).use { commonFactory ->
                val content = helper.writeDictionaryByDefaultPath(SYSTEM_PROPERTY_CFG_DIR_PATH, DICTIONARY_NAME)
                assertDictionary(commonFactory, content)

                assertConfigs(commonFactory, SYSTEM_PROPERTY_CFG_DIR_PATH)
            }
        }

        @ParameterizedTest
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CFG_DIR)
        @EnumSource(value = DictionaryHelper::class)
        fun `test with system property and custom dictionary path`(helper: DictionaryHelper) {
            val factorySettings = FactorySettings().apply {
                helper.setOption(this, CUSTOM_DIR_PATH)
            }
            CommonFactory(factorySettings).use { commonFactory ->
                val content = helper.writeDictionaryByCustomPath(CUSTOM_DIR_PATH, DICTIONARY_NAME)
                assertDictionary(commonFactory, content)

                assertConfigs(commonFactory, SYSTEM_PROPERTY_CFG_DIR_PATH)
            }
        }

        @ParameterizedTest
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CFG_DIR)
        @EnumSource(ConfigHelper::class)
        fun `test with system property and custom path for config`(helper: ConfigHelper) {
            val configPath = CUSTOM_DIR_PATH.resolve(helper.alias)
            val factorySettings = FactorySettings().apply {
                helper.setOption(this, configPath)
            }
            CommonFactory(factorySettings).use { commonFactory ->
                assertDictionaryDirs(commonFactory, SYSTEM_PROPERTY_CFG_DIR_PATH)

                val cfgBean = helper.writeConfigByCustomPath(configPath)
                assertThat(helper.loadConfig(commonFactory), samePropertyValuesAs(cfgBean))
            }
        }

        @Test
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CFG_DIR)
        fun `test with cmd config argument and system property`() {
            CommonFactory(FactorySettings().apply {
                baseConfigDir = CMD_ARG_CFG_DIR_PATH
            }).use { commonFactory ->
                assertDictionaryDirs(commonFactory, CMD_ARG_CFG_DIR_PATH)
                assertConfigs(commonFactory, CMD_ARG_CFG_DIR_PATH)
            }
        }

        @ParameterizedTest
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CFG_DIR)
        @EnumSource(value = DictionaryHelper::class)
        fun `test with cmd config argument and system property and custom dictionary path`(helper: DictionaryHelper) {
            val settings = FactorySettings().apply {
                baseConfigDir = CMD_ARG_CFG_DIR_PATH
                helper.setOption(this, CUSTOM_DIR_PATH)
            }
            CommonFactory(settings).use { commonFactory ->
                val content = helper.writeDictionaryByCustomPath(CUSTOM_DIR_PATH, DICTIONARY_NAME)
                assertDictionary(commonFactory, content)

                assertConfigs(commonFactory, CMD_ARG_CFG_DIR_PATH)
            }
        }

        @ParameterizedTest
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CFG_DIR)
        @EnumSource(ConfigHelper::class)
        fun `test with cmd config argument and system property and custom path for config`(helper: ConfigHelper) {
            val configPath = CUSTOM_DIR_PATH.resolve(helper.alias)
            CommonFactory(FactorySettings().apply {
                baseConfigDir = CMD_ARG_CFG_DIR_PATH
                helper.setOption(this, configPath)
            }).use { commonFactory ->
                assertDictionaryDirs(commonFactory, CMD_ARG_CFG_DIR_PATH)

                val cfgBean = helper.writeConfigByCustomPath(configPath)
                assertThat(helper.loadConfig(commonFactory), samePropertyValuesAs(cfgBean))
            }
        }
    }

    @Nested
    @ClearSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY)
    inner class CreateFromArguments {
        @Test
        fun `test without parameters`() {
            CommonFactory.createFromArguments().use { commonFactory ->
                assertThrowsDictionaryDir(commonFactory)
                assertThrowsConfigs(commonFactory)
            }
        }

        @Test
        fun `test with cmd config argument`() {
            CommonFactory.createFromArguments("-c", CMD_ARG_CFG_DIR).use { commonFactory ->
                assertDictionaryDirs(commonFactory, CMD_ARG_CFG_DIR_PATH)
                assertConfigs(commonFactory, CMD_ARG_CFG_DIR_PATH)
            }
        }

        @Test
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CFG_DIR)
        fun `test with system property`() {
            CommonFactory.createFromArguments().use { commonFactory ->
                assertDictionaryDirs(commonFactory, SYSTEM_PROPERTY_CFG_DIR_PATH)
                assertConfigs(commonFactory, SYSTEM_PROPERTY_CFG_DIR_PATH)
            }
        }

        @Test
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CFG_DIR)
        fun `test with cmd config argument and system property`() {
            CommonFactory.createFromArguments("-c", CMD_ARG_CFG_DIR).use { commonFactory ->
                assertDictionaryDirs(commonFactory, CMD_ARG_CFG_DIR_PATH)
                assertConfigs(commonFactory, CMD_ARG_CFG_DIR_PATH)
            }
        }
    }

    companion object {
        private const val TEMP_DIR = "build/tmp/test/common-factory"
        private const val CMD_ARG_CFG_DIR = "$TEMP_DIR/test-cmd-arg-config"
        private const val SYSTEM_PROPERTY_CFG_DIR = "$TEMP_DIR/test-system-property-config"
        private const val CUSTOM_DIR = "$TEMP_DIR/test-custom-dictionary-path"

        private val CMD_ARG_CFG_DIR_PATH = CMD_ARG_CFG_DIR.toPath()
        private val SYSTEM_PROPERTY_CFG_DIR_PATH = SYSTEM_PROPERTY_CFG_DIR.toPath()
        private val CUSTOM_DIR_PATH = CUSTOM_DIR.toPath()

        private val DICTIONARY_NAME = MAIN.name

        private fun String.toPath() = Path.of(this)

        // FIXME: find another way to check default dictionary path
        private fun assertThrowsDictionaryDir(commonFactory: CommonFactory) {
            val exception = assertThrows<IllegalStateException>("Search dictionary by default path") {
                commonFactory.readDictionary()
            }
            val message = assertNotNull(exception.message, "Exception message is null")
            assertAll(
                {
                    assertContains(
                        message,
                        other = CONFIG_DEFAULT_PATH.resolve("dictionaries").absolutePathString(),
                        message = "Exception message contains alias dictionaries path"
                    )
                },
                {
                    assertContains(
                        message,
                        other = CONFIG_DEFAULT_PATH.resolve("dictionary").absolutePathString(),
                        message = "Exception message contains type dictionaries path"
                    )
                },
                {
                    assertContains(
                        message,
                        other = CONFIG_DEFAULT_PATH.absolutePathString(),
                        message = "Exception message contains old dictionaries path"
                    )
                }
            )
        }

        private fun assertDictionaryDirs(commonFactory: CommonFactory, basePath: Path) {
            assertAll(
                {
                    val content = ALIAS.writeDictionaryByDefaultPath(basePath, MAIN.name)
                    ALIAS.assertDictionary(commonFactory, MAIN, content)
                },
                {
                    val content = TYPE.writeDictionaryByDefaultPath(basePath, INCOMING.name)
                    TYPE.assertDictionary(commonFactory, INCOMING, content)
                },
                {
                    val content = OLD.writeDictionaryByDefaultPath(basePath, OUTGOING.name)
                    OLD.assertDictionary(commonFactory, OUTGOING, content)
                }
            )
        }

        // FIXME: find another way to check default config path
        private fun assertThrowsConfigs(commonFactory: CommonFactory) {
            assertAll(
                *ConfigHelper.values().asSequence()
                    .filter { it != ConfigHelper.PROMETHEUS_CFG_ALIAS && it != ConfigHelper.BOX_CFG_ALIAS }
                    .map { helper ->
                    {
                        val exception = assertThrows<IllegalStateException>(
                            "Search config $helper by default path"
                        ) {
                            helper.loadConfig(commonFactory)
                        }
                        val message = assertNotNull(exception.message, "Exception message is null")
                        assertContains(message, other = CONFIG_DEFAULT_PATH.resolve("${helper.alias}.json").absolutePathString(), message = "Exception message contains alias config path")
                    }
                }.toList().toTypedArray()
            )

            val provider = assertProvider(commonFactory)
            assertEquals(CONFIG_DEFAULT_PATH, provider.baseDir)
            assertEquals(0, provider.customPaths.size)
        }

        private fun assertConfigs(commonFactory: CommonFactory, configPath: Path) {
            assertAll(
                ConfigHelper.values().map { helper ->
                    {
                        val cfgBean = helper.writeConfigByDefaultPath(configPath)
                        assertThat(helper.loadConfig(commonFactory), samePropertyValuesAs(cfgBean))
                    }
                }
            )
        }

        private fun assertProvider(commonFactory: CommonFactory): JsonConfigurationProvider {
            assertEquals(JsonConfigurationProvider::class, commonFactory.configurationProvider::class)
            return JsonConfigurationProvider::class.cast(commonFactory.getConfigurationProvider())
        }

        data class TestCustomConfig(val testField: String = "test-value")

        enum class ConfigHelper(
            val alias: String,
            private val configClass: Class<*>
        ) {
            RABBIT_MQ_CFG_ALIAS("rabbitMQ", RabbitMQConfiguration::class.java) {
                override fun setOption(settings: FactorySettings, filePath: Path) {
                    settings.rabbitMQ = filePath
                }

                override fun writeConfigByCustomPath(filePath: Path): RabbitMQConfiguration = RabbitMQConfiguration(
                    "test-host",
                    "test-vHost",
                    1234,
                    "test-username",
                    "test-password",
                ).also { CommonFactory.MAPPER.writeValue(filePath.toFile(), it) }
            },
            ROUTER_MQ_CFG_ALIAS("mq", MessageRouterConfiguration::class.java) {
                override fun setOption(settings: FactorySettings, filePath: Path) {
                    settings.routerMQ = filePath
                }

                override fun writeConfigByCustomPath(filePath: Path): MessageRouterConfiguration = MessageRouterConfiguration()
                    .also { CommonFactory.MAPPER.writeValue(filePath.toFile(), it) }
            },
            CONNECTION_MANAGER_CFG_ALIAS("mq_router", ConnectionManagerConfiguration::class.java) {
                override fun setOption(settings: FactorySettings, filePath: Path) {
                    settings.connectionManagerSettings = filePath
                }
                override fun writeConfigByCustomPath(filePath: Path): ConnectionManagerConfiguration = ConnectionManagerConfiguration()
                    .also { CommonFactory.MAPPER.writeValue(filePath.toFile(), it) }
            },
            GRPC_CFG_ALIAS("grpc", GrpcConfiguration::class.java) {
                override fun setOption(settings: FactorySettings, filePath: Path) {
                    settings.grpc = filePath
                }

                override fun writeConfigByCustomPath(filePath: Path): GrpcConfiguration = GrpcConfiguration()
                    .also { CommonFactory.MAPPER.writeValue(filePath.toFile(), it) }
            },
            ROUTER_GRPC_CFG_ALIAS("grpc_router", GrpcRouterConfiguration::class.java) {
                override fun setOption(settings: FactorySettings, filePath: Path) {
                    settings.routerGRPC = filePath
                }

                override fun writeConfigByCustomPath(filePath: Path): GrpcRouterConfiguration = GrpcRouterConfiguration()
                    .also { CommonFactory.MAPPER.writeValue(filePath.toFile(), it) }
            },
            CRADLE_CONFIDENTIAL_CFG_ALIAS("cradle", CradleConfidentialConfiguration::class.java) {
                override fun setOption(settings: FactorySettings, filePath: Path) {
                    settings.cradleConfidential = filePath
                }

                override fun writeConfigByCustomPath(filePath: Path): CradleConfidentialConfiguration = CradleConfidentialConfiguration(
                    "test-dataCenter",
                    "test-host",
                    "test-keyspace",
                ).also { CommonFactory.MAPPER.writeValue(filePath.toFile(), it) }
            },
            CRADLE_NON_CONFIDENTIAL_CFG_ALIAS("cradle_manager", CradleNonConfidentialConfiguration::class.java) {
                override fun setOption(settings: FactorySettings, filePath: Path) {
                    settings.cradleNonConfidential = filePath
                }

                override fun writeConfigByCustomPath(filePath: Path): CradleNonConfidentialConfiguration = CradleNonConfidentialConfiguration()
                    .also { CommonFactory.MAPPER.writeValue(filePath.toFile(), it) }
            },
            PROMETHEUS_CFG_ALIAS("prometheus", PrometheusConfiguration::class.java) {
                override fun setOption(settings: FactorySettings, filePath: Path) {
                    settings.prometheus = filePath
                }

                override fun writeConfigByCustomPath(filePath: Path): PrometheusConfiguration = PrometheusConfiguration()
                    .also { CommonFactory.MAPPER.writeValue(filePath.toFile(), it) }
            },
            BOX_CFG_ALIAS("box", BoxConfiguration::class.java) {
                override fun setOption(settings: FactorySettings, filePath: Path) {
                    settings.boxConfiguration = filePath
                }

                override fun writeConfigByCustomPath(filePath: Path): BoxConfiguration = BoxConfiguration()
                    .also { CommonFactory.MAPPER.writeValue(filePath.toFile(), it) }
            },
            CUSTOM_CFG_ALIAS("custom", TestCustomConfig::class.java) {
                override fun setOption(settings: FactorySettings, filePath: Path) {
                    settings.custom = filePath
                }

                override fun writeConfigByCustomPath(filePath: Path): TestCustomConfig = TestCustomConfig()
                    .also { CommonFactory.MAPPER.writeValue(filePath.toFile(), it) }
            };

            abstract fun setOption(settings: FactorySettings, filePath: Path)

            abstract fun writeConfigByCustomPath(filePath: Path): Any

            open fun writeConfigByDefaultPath(configPath: Path): Any = writeConfigByCustomPath(configPath.resolve("${alias}.json"))

            fun loadConfig(factory: CommonFactory): Any =
                factory.getConfigurationProvider().load(alias, configClass)
        }

        enum class DictionaryHelper {
            ALIAS {
                override fun setOption(settings: FactorySettings, path: Path) {
                    settings.dictionaryAliasesDir = path
                }

                override fun writeDictionaryByDefaultPath(basePath: Path, fileName: String): String {
                    return DictionaryHelper.writeDictionary(basePath.resolve("dictionaries").resolve(fileName))
                }

                override fun writeDictionaryByCustomPath(basePath: Path, fileName: String): String {
                    return DictionaryHelper.writeDictionary(basePath.resolve(fileName))
                }
            },
            TYPE {
                override fun setOption(settings: FactorySettings, path: Path) {
                    settings.dictionaryTypesDir = path
                }

                override fun writeDictionaryByDefaultPath(basePath: Path, fileName: String): String {
                    return DictionaryHelper.writeDictionary(basePath.resolve("dictionary").resolve(fileName.lowercase(
                        Locale.getDefault()
                    )).resolve(fileName))
                }

                override fun writeDictionaryByCustomPath(basePath: Path, fileName: String): String {
                    return DictionaryHelper.writeDictionary(basePath.resolve(fileName.lowercase(
                        Locale.getDefault()
                    )).resolve(fileName))
                }
            },
            OLD {
                override fun setOption(settings: FactorySettings, path: Path) {
                    settings.oldDictionariesDir = path
                }

                override fun writeDictionaryByDefaultPath(basePath: Path, fileName: String): String {
                    return DictionaryHelper.writeDictionary(basePath.resolve(fileName))
                }

                override fun writeDictionaryByCustomPath(basePath: Path, fileName: String): String {
                    return DictionaryHelper.writeDictionary(basePath.resolve(fileName))
                }
            };

            abstract fun setOption(settings: FactorySettings, path: Path)
            abstract fun writeDictionaryByDefaultPath(basePath: Path, fileName: String): String
            abstract fun writeDictionaryByCustomPath(basePath: Path, fileName: String): String

            fun assertDictionary(factory: CommonFactory, type: DictionaryType,  content: String) {
                assertEquals(content, factory.readDictionary(type).use { String(it.readAllBytes()) })
            }

            companion object {
                fun assertDictionary(factory: CommonFactory, content: String) {
                    assertEquals(content, factory.readDictionary().use { String(it.readAllBytes()) })
                }

                private fun writeDictionary(path: Path): String {
                    val content = RandomStringUtils.randomAlphanumeric(10)
                    path.parent.createDirectories()
                    path.writeBytes(ArchiveUtils.getGzipBase64StringEncoder().encode(content))
                    return content
                }
            }
        }
    }
}
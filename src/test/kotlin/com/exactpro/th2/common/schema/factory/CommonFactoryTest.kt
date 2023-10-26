/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.configuration.impl.JsonConfigurationProvider
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory.CUSTOM_CFG_ALIAS
import com.exactpro.th2.common.schema.factory.CommonFactory.BOX_CFG_ALIAS
import com.exactpro.th2.common.schema.factory.CommonFactory.CONFIG_DEFAULT_PATH
import com.exactpro.th2.common.schema.factory.CommonFactory.CONNECTION_MANAGER_CFG_ALIAS
import com.exactpro.th2.common.schema.factory.CommonFactory.CRADLE_CONFIDENTIAL_CFG_ALIAS
import com.exactpro.th2.common.schema.factory.CommonFactory.CRADLE_NON_CONFIDENTIAL_CFG_ALIAS
import com.exactpro.th2.common.schema.factory.CommonFactory.DICTIONARY_ALIAS_DIR_NAME
import com.exactpro.th2.common.schema.factory.CommonFactory.DICTIONARY_TYPE_DIR_NAME
import com.exactpro.th2.common.schema.factory.CommonFactory.GRPC_CFG_ALIAS
import com.exactpro.th2.common.schema.factory.CommonFactory.PROMETHEUS_CFG_ALIAS
import com.exactpro.th2.common.schema.factory.CommonFactory.RABBIT_MQ_CFG_ALIAS
import com.exactpro.th2.common.schema.factory.CommonFactory.ROUTER_GRPC_CFG_ALIAS
import com.exactpro.th2.common.schema.factory.CommonFactory.ROUTER_MQ_CFG_ALIAS
import com.exactpro.th2.common.schema.factory.CommonFactory.TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.junitpioneer.jupiter.ClearSystemProperty
import org.junitpioneer.jupiter.SetSystemProperty
import java.nio.file.Path
import kotlin.reflect.cast
import kotlin.test.assertEquals
import kotlin.test.assertNotNull


class CommonFactoryTest {

    @Nested
    @ClearSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY)
    inner class CreateByDefaultConstructor {
        @Test
        fun `test default`() {
            CommonFactory().use { commonFactory ->
                assertDictionaryDir(commonFactory, CONFIG_DEFAULT_PATH)
                assertConfigs(commonFactory)
            }
        }

        @Test
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CONFIG_DIR)
        fun `test with system property`() {
            CommonFactory().use { commonFactory ->
                assertDictionaryDir(commonFactory, SYSTEM_PROPERTY_CONFIG_DIR.toPath())
                assertConfigs(commonFactory, SYSTEM_PROPERTY_CONFIG_DIR.toPath(), false)
            }
        }
    }

    @Nested
    @ClearSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY)
    inner class CreateBySettingsConstructor {
        @Test
        fun `test default`() {
            CommonFactory(FactorySettings()).use { commonFactory ->
                assertDictionaryDir(commonFactory, CONFIG_DEFAULT_PATH)
                assertConfigs(commonFactory)
            }
        }

        @ParameterizedTest
        @ValueSource(strings = [
            ALIASES_DICTIONARY_NAME,
            TYPES_DICTIONARY_NAME,
            OLD_DICTIONARY_NAME
        ])
        fun `test custom dictionary path`(name: String) {
            val factorySettings = FactorySettings().apply {
                setCustomDictionaryPath(name)
            }
            CommonFactory(factorySettings).use { commonFactory ->
                assertCustomDictionaryPath(name, CONFIG_DEFAULT_PATH, commonFactory)
                assertConfigs(commonFactory)
            }
        }

        @ParameterizedTest
        @ValueSource(strings = [
            RABBIT_MQ_CFG_ALIAS,
            ROUTER_MQ_CFG_ALIAS,
            CONNECTION_MANAGER_CFG_ALIAS,
            GRPC_CFG_ALIAS,
            ROUTER_GRPC_CFG_ALIAS,
            CRADLE_CONFIDENTIAL_CFG_ALIAS,
            CRADLE_NON_CONFIDENTIAL_CFG_ALIAS,
            PROMETHEUS_CFG_ALIAS,
            BOX_CFG_ALIAS,
            CUSTOM_CFG_ALIAS,
        ])
        fun `test custom path for config`(alias: String) {
            val factorySettings = FactorySettings().apply {
                setCustomPathForConfig(alias)
            }
            CommonFactory(factorySettings).use { commonFactory ->
                assertDictionaryDir(commonFactory, CONFIG_DEFAULT_PATH)
                assertCustomPathForConfig(commonFactory, CONFIG_DEFAULT_PATH, alias)
            }
        }

        @Test
        fun `test with custom config path`() {
            CommonFactory(FactorySettings().apply {
                baseConfigDir = CMD_ARG_CONFIG_DIR.toPath()
            }).use { commonFactory ->
                assertDictionaryDir(commonFactory, CMD_ARG_CONFIG_DIR.toPath())
                assertConfigs(commonFactory, CMD_ARG_CONFIG_DIR.toPath(), false)
            }
        }

        @ParameterizedTest
        @ValueSource(strings = [
            ALIASES_DICTIONARY_NAME,
            TYPES_DICTIONARY_NAME,
            OLD_DICTIONARY_NAME
        ])
        fun `test with custom config path and custom dictionary path`(name: String) {
            CommonFactory(FactorySettings().apply {
                baseConfigDir = CMD_ARG_CONFIG_DIR.toPath()
                setCustomDictionaryPath(name)
            }).use { commonFactory ->
                assertCustomDictionaryPath(name, CMD_ARG_CONFIG_DIR.toPath(), commonFactory)
                assertConfigs(commonFactory, CMD_ARG_CONFIG_DIR.toPath(), false)
            }
        }

        @ParameterizedTest
        @ValueSource(strings = [
            RABBIT_MQ_CFG_ALIAS,
            ROUTER_MQ_CFG_ALIAS,
            CONNECTION_MANAGER_CFG_ALIAS,
            GRPC_CFG_ALIAS,
            ROUTER_GRPC_CFG_ALIAS,
            CRADLE_CONFIDENTIAL_CFG_ALIAS,
            CRADLE_NON_CONFIDENTIAL_CFG_ALIAS,
            PROMETHEUS_CFG_ALIAS,
            BOX_CFG_ALIAS,
            CUSTOM_CFG_ALIAS,
        ])
        fun `test with custom config path and custom path for config`(alias: String) {
            CommonFactory(FactorySettings().apply {
                baseConfigDir = CMD_ARG_CONFIG_DIR.toPath()
                setCustomPathForConfig(alias)
            }).use { commonFactory ->
                assertDictionaryDir(commonFactory, CMD_ARG_CONFIG_DIR.toPath())
                assertCustomPathForConfig(commonFactory, CMD_ARG_CONFIG_DIR.toPath(), alias)
            }
        }

        @Test
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CONFIG_DIR)
        fun `test with system property`() {
            CommonFactory(FactorySettings()).use { commonFactory ->
                assertDictionaryDir(commonFactory, SYSTEM_PROPERTY_CONFIG_DIR.toPath())
                assertConfigs(commonFactory, SYSTEM_PROPERTY_CONFIG_DIR.toPath(), false)
            }
        }

        @ParameterizedTest
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CONFIG_DIR)
        @ValueSource(strings = [
            ALIASES_DICTIONARY_NAME,
            TYPES_DICTIONARY_NAME,
            OLD_DICTIONARY_NAME
        ])
        fun `test with system property and custom dictionary path`(name: String) {
            val factorySettings = FactorySettings().apply {
                setCustomDictionaryPath(name)
            }
            CommonFactory(factorySettings).use { commonFactory ->
                assertCustomDictionaryPath(name, SYSTEM_PROPERTY_CONFIG_DIR.toPath(), commonFactory)
                assertConfigs(commonFactory, SYSTEM_PROPERTY_CONFIG_DIR.toPath(), false)
            }
        }

        @ParameterizedTest
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CONFIG_DIR)
        @ValueSource(strings = [
            RABBIT_MQ_CFG_ALIAS,
            ROUTER_MQ_CFG_ALIAS,
            CONNECTION_MANAGER_CFG_ALIAS,
            GRPC_CFG_ALIAS,
            ROUTER_GRPC_CFG_ALIAS,
            CRADLE_CONFIDENTIAL_CFG_ALIAS,
            CRADLE_NON_CONFIDENTIAL_CFG_ALIAS,
            PROMETHEUS_CFG_ALIAS,
            BOX_CFG_ALIAS,
            CUSTOM_CFG_ALIAS,
        ])
        fun `test with system property and custom path for config`(alias: String) {
            val factorySettings = FactorySettings().apply {
                setCustomPathForConfig(alias)
            }
            CommonFactory(factorySettings).use { commonFactory ->
                assertDictionaryDir(commonFactory, SYSTEM_PROPERTY_CONFIG_DIR.toPath())
                assertCustomPathForConfig(commonFactory, SYSTEM_PROPERTY_CONFIG_DIR.toPath(), alias)
            }
        }

        @Test
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CONFIG_DIR)
        fun `test with cmd config argument and system property`() {
            CommonFactory(FactorySettings().apply {
                baseConfigDir = CMD_ARG_CONFIG_DIR.toPath()
            }).use { commonFactory ->
                assertDictionaryDir(commonFactory, CMD_ARG_CONFIG_DIR.toPath())
                assertConfigs(commonFactory, CMD_ARG_CONFIG_DIR.toPath(), false)
            }
        }

        @ParameterizedTest
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CONFIG_DIR)
        @ValueSource(strings = [
            ALIASES_DICTIONARY_NAME,
            TYPES_DICTIONARY_NAME,
            OLD_DICTIONARY_NAME
        ])
        fun `test with cmd config argument and system property and custom dictionary path`(name: String) {
            CommonFactory(FactorySettings().apply {
                baseConfigDir = CMD_ARG_CONFIG_DIR.toPath()
                setCustomDictionaryPath(name)
            }).use { commonFactory ->
                assertCustomDictionaryPath(name, CMD_ARG_CONFIG_DIR.toPath(), commonFactory)
                assertConfigs(commonFactory, CMD_ARG_CONFIG_DIR.toPath(), false)
            }
        }

        @ParameterizedTest
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CONFIG_DIR)
        @ValueSource(strings = [
            RABBIT_MQ_CFG_ALIAS,
            ROUTER_MQ_CFG_ALIAS,
            CONNECTION_MANAGER_CFG_ALIAS,
            GRPC_CFG_ALIAS,
            ROUTER_GRPC_CFG_ALIAS,
            CRADLE_CONFIDENTIAL_CFG_ALIAS,
            CRADLE_NON_CONFIDENTIAL_CFG_ALIAS,
            PROMETHEUS_CFG_ALIAS,
            BOX_CFG_ALIAS,
            CUSTOM_CFG_ALIAS,
        ])
        fun `test with cmd config argument and system property and custom path for config`(alias: String) {
            CommonFactory(FactorySettings().apply {
                baseConfigDir = CMD_ARG_CONFIG_DIR.toPath()
                setCustomPathForConfig(alias)
            }).use { commonFactory ->
                assertDictionaryDir(commonFactory, CMD_ARG_CONFIG_DIR.toPath())
                assertCustomPathForConfig(commonFactory, CMD_ARG_CONFIG_DIR.toPath(), alias)
            }
        }
    }

    @Nested
    @ClearSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY)
    inner class CreateFromArguments {
        @Test
        fun `test without parameters`() {
            CommonFactory.createFromArguments().use { commonFactory ->
                assertDictionaryDir(commonFactory, CONFIG_DEFAULT_PATH)
                assertConfigs(commonFactory)
            }
        }

        @Test
        fun `test with cmd config argument`() {
            CommonFactory.createFromArguments("-c", CMD_ARG_CONFIG_DIR).use { commonFactory ->
                assertDictionaryDir(commonFactory, CMD_ARG_CONFIG_DIR.toPath())
                assertConfigs(commonFactory, CMD_ARG_CONFIG_DIR.toPath(), false)
            }
        }

        @Test
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CONFIG_DIR)
        fun `test with system property`() {
            CommonFactory.createFromArguments().use { commonFactory ->
                assertDictionaryDir(commonFactory, SYSTEM_PROPERTY_CONFIG_DIR.toPath())
                assertConfigs(commonFactory, SYSTEM_PROPERTY_CONFIG_DIR.toPath(), false)
            }
        }

        @Test
        @SetSystemProperty(key = TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY, value = SYSTEM_PROPERTY_CONFIG_DIR)
        fun `test with cmd config argument and system property`() {
            CommonFactory.createFromArguments("-c", CMD_ARG_CONFIG_DIR).use { commonFactory ->
                assertDictionaryDir(commonFactory, CMD_ARG_CONFIG_DIR.toPath())
                assertConfigs(commonFactory, CMD_ARG_CONFIG_DIR.toPath(), false)
            }
        }
    }

    companion object {
        private const val CMD_ARG_CONFIG_DIR = "src/test/resources/test_cmd_arg_config"
        private const val SYSTEM_PROPERTY_CONFIG_DIR = "src/test/resources/test_system_property_config"
        private val CUSTOM_DIR = Path.of("dictionary/custom/path")

        private const val ALIASES_DICTIONARY_NAME = "aliases"
        private const val TYPES_DICTIONARY_NAME = "types"
        private const val OLD_DICTIONARY_NAME = "old"

        private val CONFIG_NAME_TO_COMMON_FACTORY_SUPPLIER: Map<String, DictionaryDirMetadata> = hashMapOf(
            ALIASES_DICTIONARY_NAME to DictionaryDirMetadata(DICTIONARY_ALIAS_DIR_NAME, FactorySettings::dictionaryAliasesDir, CommonFactory::getPathToDictionaryAliasesDir),
            TYPES_DICTIONARY_NAME to DictionaryDirMetadata(DICTIONARY_TYPE_DIR_NAME, FactorySettings::dictionaryTypesDir, CommonFactory::getPathToDictionaryTypesDir),
            OLD_DICTIONARY_NAME to DictionaryDirMetadata("", FactorySettings::oldDictionariesDir, CommonFactory::getOldPathToDictionariesDir),
        )

        private val CONFIG_ALIASES: Map<String, ConfigMetadata> = hashMapOf(
            RABBIT_MQ_CFG_ALIAS to ConfigMetadata(FactorySettings::rabbitMQ),
            ROUTER_MQ_CFG_ALIAS to ConfigMetadata(FactorySettings::routerMQ),
            CONNECTION_MANAGER_CFG_ALIAS to ConfigMetadata(FactorySettings::connectionManagerSettings),
            GRPC_CFG_ALIAS to ConfigMetadata(FactorySettings::grpc),
            ROUTER_GRPC_CFG_ALIAS to ConfigMetadata(FactorySettings::routerGRPC),
            CRADLE_CONFIDENTIAL_CFG_ALIAS to ConfigMetadata(FactorySettings::cradleConfidential),
            CRADLE_NON_CONFIDENTIAL_CFG_ALIAS to ConfigMetadata(FactorySettings::cradleNonConfidential),
            PROMETHEUS_CFG_ALIAS to ConfigMetadata(FactorySettings::prometheus),
            BOX_CFG_ALIAS to ConfigMetadata(FactorySettings::boxConfiguration),
            CUSTOM_CFG_ALIAS to ConfigMetadata(FactorySettings::custom),
        )

        private fun String.toPath() = Path.of(this)

        private fun assertDictionaryDir(commonFactory: CommonFactory, configPath: Path) {
            CONFIG_NAME_TO_COMMON_FACTORY_SUPPLIER.forEach { (name, dictionaryDirMetadata) ->
                assertEquals(
                    configPath.resolve(dictionaryDirMetadata.dirName),
                    dictionaryDirMetadata.getPath.invoke(commonFactory),
                    "Configured config path: $configPath, config name: $name"
                )
            }
        }

        private fun assertConfigs(commonFactory: CommonFactory) {
            val provider = assertProvider(commonFactory)
            assertEquals(CONFIG_DEFAULT_PATH, provider.baseDir)
            assertEquals(0, provider.customPaths.size)
        }
        private fun assertConfigs(commonFactory: CommonFactory, configPath: Path, customPaths: Boolean) {
            val provider = assertProvider(commonFactory)
            assertEquals(configPath, provider.baseDir)

            if (customPaths) {
                assertEquals(CONFIG_ALIASES.size, provider.customPaths.size)

                CONFIG_ALIASES.keys.forEach { alias ->
                    val path = assertNotNull(provider.customPaths[alias])
                    assertEquals(configPath, path.parent, "Configured config path: $configPath")
                }
            } else {
                assertEquals(0, provider.customPaths.size)
            }
        }

        private fun assertProvider(commonFactory: CommonFactory): JsonConfigurationProvider {
            assertEquals(JsonConfigurationProvider::class, commonFactory.configurationProvider::class)
            return JsonConfigurationProvider::class.cast(commonFactory.getConfigurationProvider())
        }

        private fun assertCustomPathForConfig(commonFactory: CommonFactory, basePath: Path, alias: String) {
            val provider = assertProvider(commonFactory)
            assertEquals(basePath, provider.baseDir)
            assertEquals(1, provider.customPaths.size)
            assertEquals(CUSTOM_DIR, provider.customPaths[alias])
        }

        private fun FactorySettings.setCustomPathForConfig(alias: String) {
            CONFIG_ALIASES.asSequence()
                .find { (optionName, _) -> optionName == alias }
                ?.value?.setPath?.invoke(this, CUSTOM_DIR)
        }

        private fun assertCustomDictionaryPath(
            name: String,
            basePath: Path,
            commonFactory: CommonFactory
        ) {
            CONFIG_NAME_TO_COMMON_FACTORY_SUPPLIER.forEach { (optionName, dictionaryDirMetadata) ->
                if (optionName == name) {
                    assertEquals(
                        CUSTOM_DIR,
                        dictionaryDirMetadata.getPath.invoke(commonFactory),
                        "Configured config path: $CUSTOM_DIR, config optionName: $optionName"
                    )
                } else {
                    assertEquals(
                        basePath.resolve(dictionaryDirMetadata.dirName),
                        dictionaryDirMetadata.getPath.invoke(commonFactory),
                        "Configured config path: $basePath, config optionName: $optionName"
                    )
                }
            }
        }

        private fun FactorySettings.setCustomDictionaryPath(
            name: String
        ) {
            CONFIG_NAME_TO_COMMON_FACTORY_SUPPLIER.asSequence()
                .find { (optionName, _) -> optionName == name }
                ?.value?.setPath?.invoke(this, CUSTOM_DIR)
        }

        private data class DictionaryDirMetadata(
            val dirName: String,
            val setPath: FactorySettings.(Path) -> Unit,
            val getPath: CommonFactory.() -> Path,
        )

        private data class ConfigMetadata(
            val setPath: FactorySettings.(Path) -> Unit,
        )
    }
}
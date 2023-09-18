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

import com.exactpro.cradle.cassandra.CassandraStorageSettings
import com.exactpro.th2.common.metrics.PrometheusConfiguration
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration
import com.exactpro.th2.common.schema.cradle.CradleConfidentialConfiguration
import com.exactpro.th2.common.schema.cradle.CradleNonConfidentialConfiguration
import com.exactpro.th2.common.schema.factory.CommonFactory.CONFIG_DEFAULT_PATH
import com.exactpro.th2.common.schema.factory.CommonFactory.CUSTOM_FILE_NAME
import com.exactpro.th2.common.schema.factory.CommonFactory.DICTIONARY_ALIAS_DIR_NAME
import com.exactpro.th2.common.schema.factory.CommonFactory.DICTIONARY_TYPE_DIR_NAME
import com.exactpro.th2.common.schema.factory.CommonFactory.TH2_COMMON_CONFIGURATION_DIRECTORY_ENVIRONMENT_VARIABLE
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables
import uk.org.webcompere.systemstubs.jupiter.SystemStub
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertNotNull


@ExtendWith(SystemStubsExtension::class)
class CommonFactoryTest {

    @SystemStub
    private val environmentVariables = EnvironmentVariables()

    @Test
    fun `test load config by default path (default constructor)`() {
        CommonFactory().use { commonFactory ->
            assertConfigs(commonFactory, CONFIG_DEFAULT_PATH)
        }
    }

    @Test
    fun `test load config by default path (createFromArguments(empty))`() {
        CommonFactory.createFromArguments().use { commonFactory ->
            assertConfigs(commonFactory, CONFIG_DEFAULT_PATH)
        }
    }

    @Test
    fun `test load config by custom path (createFromArguments(not empty))`() {
        CommonFactory.createFromArguments("-c", CONFIG_DIR_IN_RESOURCE.toString()).use { commonFactory ->
            assertConfigs(commonFactory, CONFIG_DIR_IN_RESOURCE)
        }
    }

    @Test
    fun `test load config by environment variable path (default constructor)`() {
        environmentVariables.set(TH2_COMMON_CONFIGURATION_DIRECTORY_ENVIRONMENT_VARIABLE, CONFIG_DIR_IN_RESOURCE.toString())
        CommonFactory().use { commonFactory ->
            assertConfigs(commonFactory, CONFIG_DIR_IN_RESOURCE)
        }
    }

    @Test
    fun `test load config by environment variable path (createFromArguments(empty))`() {
        environmentVariables.set(TH2_COMMON_CONFIGURATION_DIRECTORY_ENVIRONMENT_VARIABLE, CONFIG_DIR_IN_RESOURCE.toString())
        CommonFactory.createFromArguments().use { commonFactory ->
            assertConfigs(commonFactory, CONFIG_DIR_IN_RESOURCE)
        }
    }

    @Test
    fun `test load config by custom path (createFromArguments(not empty) + environment variable)`() {
        environmentVariables.set(TH2_COMMON_CONFIGURATION_DIRECTORY_ENVIRONMENT_VARIABLE, CONFIG_DIR_IN_RESOURCE.toString())
        CommonFactory.createFromArguments("-c", CONFIG_DIR_IN_RESOURCE.toString()).use { commonFactory ->
            assertConfigs(commonFactory, CONFIG_DIR_IN_RESOURCE)
        }
    }


    private fun assertConfigs(commonFactory: CommonFactory, configPath: Path) {
        CONFIG_NAME_TO_COMMON_FACTORY_SUPPLIER.forEach { (configName, actualPathSupplier) ->
            assertEquals(configPath.resolve(configName), commonFactory.actualPathSupplier(), "Configured config path: $configPath, config name: $configName")
        }
        assertConfigurationManager(commonFactory, configPath)
    }

    private fun assertConfigurationManager(commonFactory: CommonFactory, configPath: Path) {
        CONFIG_CLASSES.forEach { clazz ->
            assertNotNull(commonFactory.configurationManager[clazz])
            assertEquals(configPath, commonFactory.configurationManager[clazz]?.parent , "Configured config path: $configPath, config class: $clazz")
        }
    }

    companion object {
        private val CONFIG_DIR_IN_RESOURCE = Path.of("src/test/resources/test_common_factory_load_configs")

        private val CONFIG_NAME_TO_COMMON_FACTORY_SUPPLIER: Map<String, CommonFactory.() -> Path> = mapOf(
            CUSTOM_FILE_NAME to { pathToCustomConfiguration },
            DICTIONARY_ALIAS_DIR_NAME to { pathToDictionaryAliasesDir },
            DICTIONARY_TYPE_DIR_NAME to { pathToDictionaryTypesDir },
        )

        private val CONFIG_CLASSES: Set<Class<*>> = setOf(
            RabbitMQConfiguration::class.java,
            MessageRouterConfiguration::class.java,
            ConnectionManagerConfiguration::class.java,
            GrpcConfiguration::class.java,
            GrpcRouterConfiguration::class.java,
            CradleConfidentialConfiguration::class.java,
            CradleNonConfidentialConfiguration::class.java,
            CassandraStorageSettings::class.java,
            PrometheusConfiguration::class.java,
            BoxConfiguration::class.java,
        )
    }
}
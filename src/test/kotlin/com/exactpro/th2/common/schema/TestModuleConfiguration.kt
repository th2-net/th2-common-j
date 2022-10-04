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

package com.exactpro.th2.common.schema

import com.exactpro.th2.common.module.provider.FileConfigurationProvider
import com.exactpro.th2.common.schema.factory.CommonFactory
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class TestModuleConfiguration {

    @Test
    fun `test configuration from command line`() {
        val factory = CommonFactory.createFromArguments(
            "-c",
            "src/test/resources/test_module_configuration",
            "-configurationProviderClass",
            "com.exactpro.th2.common.module.provider.FileConfigurationProvider",
        )

        assertTrue(factory.configurationProvider is FileConfigurationProvider)
    }

    @Test
    fun `test configuration with provider config from command line`() {
        val factory = CommonFactory.createFromArguments(
            "-c",
            "src/test/resources/test_module_configuration",
            "-configurationProviderClass",
            "com.exactpro.th2.common.module.provider.FileConfigurationProvider",
            "-file-provider-extension",
            "json"
        )

        assertTrue(factory.configurationProvider is FileConfigurationProvider)
    }

    @Test
    fun `test provider default value`() {
        val factory = CommonFactory.createFromArguments("-c", "src/test/resources/test_module_configuration")

        assertTrue(factory.configurationProvider is FileConfigurationProvider)
    }

}
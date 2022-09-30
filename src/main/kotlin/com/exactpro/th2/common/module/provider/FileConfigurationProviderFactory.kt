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
import com.exactpro.th2.common.schema.strategy.route.json.RoutingStrategyModule
import com.exactpro.th2.common.util.createLongOption
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Options

open class FileConfigurationProviderFactory: ConfigurationProviderFactory {

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

    /**
     * Adds options to [Options]
     *
     * See [companion object of this class][Companion]
     */
    override fun addOwnOptionsToCmd(options: Options) {
        createLongOption(options, PATH_OPTION)
        createLongOption(options, EXTENSION_OPTION)
    }

    /**
     * Parses [CommandLine] object.
     *
     * You can check parameters names ane their default values in [companion object of this class][Companion].
     */
    override fun parseCommandLine(cmd: CommandLine): Array<String> {
        return arrayOf(
            cmd.getOptionValue(PATH_OPTION, DEFAULT_PATH),
            cmd.getOptionValue(EXTENSION_OPTION, DEFAULT_EXTENSION)
        )
    }

    /**
     * Accepts two parameters:
     *
     * args[0] - directory where provider will search files
     * args[1] - extension of file without dot
     */
    override fun initProvider(args: Array<String>): ConfigurationProvider {
        val firstParam = args.getOrNull(0)
        val secondParam = args.getOrNull(1)

        return FileConfigurationProvider(objectMapper,
            configurationDir = firstParam ?: DEFAULT_PATH,
            fileExtension = secondParam ?: DEFAULT_EXTENSION
        )
    }

    companion object {
        const val PATH_OPTION = "file-provider-path"
        const val EXTENSION_OPTION = "file-provider-extension"

        const val DEFAULT_PATH = "/opt/th2/config"
        const val DEFAULT_EXTENSION = "json"
    }

}
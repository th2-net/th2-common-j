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

package com.exactpro.th2.common.schema.configuration

import java.io.InputStream
import java.util.function.Function
import java.util.function.Supplier

interface IConfigurationProvider {
    /**
     * Loads instance of [configClass] from a resource related to [alias].
     * Configuration provider parses resource using internal parser.
     * You can load the same resources using different classes.
     * @param configClass - target class of loading config
     * @param alias - alias of related resources
     * @param default - supplier of default instance of configuration class. Pass `null` if configuration must be loaded
     */
    fun <T : Any> load(alias: String, configClass: Class<T>, default: Supplier<T>?): T
    /**
     * Loads instance of [configClass] from a resource related to [alias].
     * Configuration provider parses resource using internal parser.
     * You can load the same resources using different classes.
     * @param configClass - target class of loading config
     * @param alias - alias of related resources
     */
    fun <T : Any> load(alias: String, configClass: Class<T>): T = load(alias, configClass, null)
    /**
     * Loads instance using [parser] from a resource related to [alias].
     * You can load the same resources by different classes.
     * @param alias - alias of related resources
     * @param parser - function to parse [InputStream] to config instance
     * @param default - supplier of default instance of configuration class. Pass `null` if configuration must be loaded
     */
    fun <T : Any> load(alias: String, parser: Function<InputStream, T>, default: Supplier<T>?): T
    /**
     * Loads instance using [parser] from a resource related to [alias].
     * You can load the same resources by different classes.
     * @param alias - alias of related resources
     * @param parser - function to parse [InputStream] to config instance
     */
    fun <T : Any> load(alias: String, parser: Function<InputStream, T>): T = load(alias, parser, null as Supplier<T>?)
}
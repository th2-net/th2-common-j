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

package com.exactpro.th2.common.cli

import com.beust.jcommander.Parameter


class CommonFactoryArgs {

    @Parameter(names = ["-c", "--configs"])
    lateinit var config: String

    @Parameter(names = ["--rabbitConfiguration"])
    lateinit var rabbitConfiguration: String

    @Parameter(names = ["--messageRouterConfiguration"])
    lateinit var messageRouterConfiguration: String

    @Parameter(names = ["--grpcRouterConfiguration"])
    lateinit var grpcRouterConfiguration: String

    @Parameter(names = ["--grpcConfiguration"])
    lateinit var grpcConfiguration: String

    @Parameter(names = ["--grpcRouterConfig"])
    lateinit var grpcRouterConfig: String

    @Parameter(names = ["--customConfiguration"])
    lateinit var customConfiguration: String

    @Parameter(names = ["--dictionariesDir"])
    lateinit var dictionariesDir: String

    @Parameter(names = ["--prometheusConfiguration"])
    lateinit var prometheusConfiguration: String

    @Parameter(names = ["--boxConfiguration"])
    lateinit var boxConfiguration: String

    @Parameter(names = ["--namespace"])
    lateinit var namespace: String

    @Parameter(names = ["--boxName"])
    lateinit var boxName: String

    @Parameter(names = ["--contextName"])
    lateinit var contextName: String

    @Parameter(names = ["--dictionaries"])
    lateinit var dictionaries: List<String>

    @Parameter(names = ["--connectionManagerConfiguration"])
    lateinit var connectionManagerConfiguration: String

    @Parameter(names = ["--configurationProviderClass"])
    lateinit var configurationProviderClass: String

}
/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.configuration

import com.fasterxml.jackson.annotation.JsonAnySetter
import org.slf4j.Logger
import org.slf4j.LoggerFactory

open class Configuration {

    protected val logger: Logger = LoggerFactory.getLogger(this::class.java)

    @JsonAnySetter
    fun setField(name: String, value: Any?) {
        logger.warn("Ignore unknown field with name '{}' in configuration class '{}'", name, this::class.java.name)
    }
}
/*
 *  Copyright 2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.dictionary.DictionaryType
import java.io.InputStream

interface IDictionaryProvider {
    fun aliases(): Set<String>
    fun load(alias: String): InputStream
    @Deprecated("Load dictionary by type is deprecated, please use load by alias")
    fun load(type: DictionaryType): InputStream
    @Deprecated("Load single dictionary is deprecated, please use load by alias")
    fun load(): InputStream
}
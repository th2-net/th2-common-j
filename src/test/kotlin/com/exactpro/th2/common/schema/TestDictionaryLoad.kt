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

package com.exactpro.th2.common.schema

import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.CommonFactory
import org.junit.jupiter.api.Test

class TestDictionaryLoad {

    @Test
    fun `test file load dictionary`() {
        val factory = CommonFactory.createFromArguments("-c", "src/test/resources/test_load_dictionaries")

        factory.readDictionary().use {
            assert(String(it.readAllBytes()) == "test file")
        }
    }

    @Test
    fun `test folder load dictionary`() {
        val factory = CommonFactory.createFromArguments("-c", "src/test/resources/test_load_dictionaries")

        factory.readDictionary(DictionaryType.LEVEL1).use {
            assert(String(it.readAllBytes()) == "test file")
        }
    }

}
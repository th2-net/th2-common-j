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

import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.factory.FactorySettings
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.file.Path

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

    @Test
    fun `test folder load dictionaries by alias`() {
        val factory = CommonFactory.createFromArguments("-c", "src/test/resources/test_load_dictionaries")

        Assertions.assertDoesNotThrow {
            factory.loadDictionary("test_alias_2").use {
                assert(String(it.readAllBytes()) == "test file")
            }
        }
    }

    @Test
    fun `test folder load all dictionary aliases`() {
        val factory = CommonFactory.createFromArguments("-c", "src/test/resources/test_load_dictionaries")
        val expectedNames = listOf("test_alias_1", "test_alias_2", "test_alias_3", "test_alias_4")
        val names = factory.loadDictionaryAliases()
        Assertions.assertEquals(4, names.size)
        Assertions.assertTrue(names.containsAll(expectedNames))
    }

    @Test
    fun `test folder load single dictionary`() {
        val factory = CommonFactory.createFromArguments("-c", "src/test/resources/test_load_dictionaries")

        Assertions.assertThrows(IllegalStateException::class.java) {
            factory.loadDictionary()
        }

        val customSettings = FactorySettings().apply { dictionaryAliasesDir = Path.of("src/test/resources/test_load_dictionaries/single_dictionary") }
        val customFactory = CommonFactory(customSettings)

        customFactory.loadDictionary().use {
            assert(String(it.readAllBytes()) == "test file")
        }
    }

}
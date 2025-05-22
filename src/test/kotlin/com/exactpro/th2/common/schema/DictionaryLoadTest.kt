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
import com.exactpro.th2.common.schema.util.ArchiveUtils
import org.apache.commons.lang3.RandomStringUtils
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.file.Path
import kotlin.io.path.absolutePathString
import kotlin.io.path.createDirectories
import kotlin.io.path.writeBytes
import kotlin.io.path.writeText
import kotlin.test.assertEquals

@Suppress("DEPRECATION")
class DictionaryLoadTest {

    @TempDir
    lateinit var tempDir: Path
    
    @BeforeEach
    fun beforeEach() {
        writePrometheus(tempDir)
    }

    //--//--//--readDictionary()--//--//--//

    @Test
    fun `test read dictionary from old dictionary dir`() {
        val content = writeDictionary(tempDir.resolve(Path.of("MAIN")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            assertEquals(content, commonFactory.readDictionary().use { String(it.readAllBytes()) })
        }
    }

    @Test
    fun `test read dictionary from old dictionary dir - file name mismatch`() {
        writeDictionary(tempDir.resolve(Path.of("main")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            Assertions.assertThrows(IllegalStateException::class.java) {
                commonFactory.readDictionary()
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["MAIN", "main", "test-dictionary"])
    fun `test read dictionary from type dictionary dir`(fileName: String) {
        val content = writeDictionary(tempDir.resolve(Path.of("dictionary", "main", fileName)))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            assertEquals(content, commonFactory.readDictionary().use { String(it.readAllBytes()) })
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["MAIN", "main", "test-dictionary"])
    fun `test read dictionary from type dictionary dir - dictionary name mismatch`(
        fileName: String,
        
    ) {
        writeDictionary(tempDir.resolve(Path.of("dictionary", "MAIN", fileName)))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            Assertions.assertThrows(IllegalStateException::class.java) {
                commonFactory.readDictionary()
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["MAIN", "main", "MAIN.xml", "main.json"])
    fun `test read dictionary from alias dictionary dir`(fileName: String) {
        val content = writeDictionary(tempDir.resolve(Path.of("dictionaries", fileName)))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            assertEquals(content, commonFactory.readDictionary().use { String(it.readAllBytes()) })
        }
    }

    //--//--//--readDictionary(<type>)--//--//--//

    @Test
    fun `test read dictionary by type from old dictionary dir`() {
        val content = writeDictionary(tempDir.resolve(Path.of("INCOMING")))
        writeDictionary(tempDir.resolve(Path.of("MAIN")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            assertEquals(
                content,
                commonFactory.readDictionary(DictionaryType.INCOMING).use { String(it.readAllBytes()) })
        }
    }

    @Test
    fun `test read dictionary by type from old dictionary dir - file name mismatch`() {
        writeDictionary(tempDir.resolve(Path.of("incoming")))
        writeDictionary(tempDir.resolve(Path.of("MAIN")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            Assertions.assertThrows(IllegalStateException::class.java) {
                commonFactory.readDictionary(DictionaryType.INCOMING)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["INCOMING", "incoming", "test-dictionary"])
    fun `test read dictionary by type from type dictionary dir`(fileName: String) {
        val content = writeDictionary(tempDir.resolve(Path.of("dictionary", "incoming", fileName)))
        writeDictionary(tempDir.resolve(Path.of("dictionary", "main", "MAIN")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            assertEquals(
                content,
                commonFactory.readDictionary(DictionaryType.INCOMING).use { String(it.readAllBytes()) })
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["INCOMING", "incoming", "test-dictionary"])
    fun `test read dictionary by type from type dictionary dir - dictionary name mismatch`(
        fileName: String,
        
    ) {
        writeDictionary(tempDir.resolve(Path.of("dictionary", "INCOMING", fileName)))
        writeDictionary(tempDir.resolve(Path.of("dictionary", "main", "MAIN")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            Assertions.assertThrows(IllegalStateException::class.java) {
                commonFactory.readDictionary(DictionaryType.INCOMING)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["INCOMING", "incoming", "INCOMING.xml", "incoming.json"])
    fun `test read dictionary by type from alias dictionary dir`(fileName: String) {
        val content = writeDictionary(tempDir.resolve(Path.of("dictionaries", fileName)))
        writeDictionary(tempDir.resolve(Path.of("dictionaries", "MAIN")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            assertEquals(
                content,
                commonFactory.readDictionary(DictionaryType.INCOMING).use { String(it.readAllBytes()) })
        }
    }

    //--//--//--loadSingleDictionary()--//--//--//

    @Test
    fun `test load single dictionary from alias dictionary dir`() {
        val content = writeDictionary(tempDir.resolve(Path.of("dictionaries", "test-dictionary")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            assertEquals(content, commonFactory.loadSingleDictionary().use { String(it.readAllBytes()) })
        }
    }

    @Test
    fun `test load single dictionary from alias dictionary dir - several dictionaries`() {
        writeDictionary(tempDir.resolve(Path.of("dictionaries", "test-dictionary-1")))
        writeDictionary(tempDir.resolve(Path.of("dictionaries", "test-dictionary-2")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            Assertions.assertThrows(IllegalStateException::class.java) {
                commonFactory.loadSingleDictionary()
            }
        }
    }

    @Test
    fun `test load single dictionary from alias dictionary dir - several dictionaries with name in different case`() {
        writeDictionary(tempDir.resolve(Path.of("dictionaries", "test-dictionary")))
        writeDictionary(tempDir.resolve(Path.of("dictionaries", "TEST-DICTIONARY")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            Assertions.assertThrows(IllegalStateException::class.java) {
                commonFactory.loadSingleDictionary()
            }
        }
    }

    //--//--//--loadDictionary(<alias>)--//--//--//

    @ParameterizedTest
    @ValueSource(strings = ["TEST-ALIAS", "test-alias"])
    fun `test load dictionary by alias from alias dictionary dir`(fileName: String) {
        val content = writeDictionary(tempDir.resolve(Path.of("dictionaries", fileName)))
        writeDictionary(tempDir.resolve(Path.of("dictionaries", "test-dictionary")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            assertEquals(content, commonFactory.loadDictionary("test-alias").use { String(it.readAllBytes()) })
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["TEST-DICTIONARY", "test-dictionary"])
    fun `test load dictionary by alias from alias dictionary dir - several dictionaries with name in different case`(
        alias: String,
        
    ) {
        writeDictionary(tempDir.resolve(Path.of("dictionaries", "test-dictionary")))
        writeDictionary(tempDir.resolve(Path.of("dictionaries", "TEST-DICTIONARY")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            Assertions.assertThrows(IllegalStateException::class.java) {
                commonFactory.loadDictionary(alias)
            }
        }
    }

    //--//--//--loadAliases--//--//--//

    @Test
    fun `test dictionary aliases from alias dictionary dir`() {
        val alias1 = "test-dictionary-1"
        val alias2 = "TEST-DICTIONARY-2"
        val file1 = "$alias1.xml"
        val file2 = "$alias2.json"
        writeDictionary(tempDir.resolve(Path.of("dictionaries", file1)))
        writeDictionary(tempDir.resolve(Path.of("dictionaries", file2)))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            assertEquals(setOf(alias1, alias2.lowercase()), commonFactory.dictionaryAliases)
        }
    }

    @Test
    fun `test dictionary aliases from alias dictionary dir - empty directory`() {
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            assertEquals(emptySet(), commonFactory.dictionaryAliases)
        }
    }

    @Test
    fun `test dictionary aliases from alias dictionary dir - several dictionaries with name in different case`() {
        writeDictionary(tempDir.resolve(Path.of("dictionaries", "test-dictionary")))
        writeDictionary(tempDir.resolve(Path.of("dictionaries", "TEST-DICTIONARY")))
        CommonFactory.createFromArguments("-c", tempDir.absolutePathString()).use { commonFactory ->
            Assertions.assertThrows(IllegalStateException::class.java) {
                commonFactory.dictionaryAliases
            }
        }
    }

    //--//--//--Others--//--//--//

    private fun writePrometheus(cfgPath: Path) {
        cfgPath.resolve("prometheus.json").writeText("{\"enabled\":false}")
    }

    private fun writeDictionary(path: Path): String {
        val content = RandomStringUtils.randomAlphanumeric(10)
        path.parent.createDirectories()
        path.writeBytes(ArchiveUtils.getGzipBase64StringEncoder().encode(content))
        return content
    }

}
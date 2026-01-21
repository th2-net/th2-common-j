/*
 * Copyright 2020-2026 Exactpro (Exactpro Systems Limited)
 *
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.custom

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

class TestMessageConverterLambdaDelegate {
    @Test
    fun `all methods return expected value`() {
        val converter: MessageConverter<Byte> = MessageConverter.create({ byteArrayOf(it) }, { it[0] }, { "Value$it" }, { "Value$it" }, { it.toInt() })
        assertAll(
            { assertArrayEquals(byteArrayOf(42), converter.toByteArray(42)) },
            { assertEquals(42, converter.fromByteArray(byteArrayOf(42))) },
            { assertEquals("Value42", converter.toTraceString(42)) },
            { assertEquals("Value42", converter.toDebugString(42)) },
            { assertEquals(42, converter.extractCount(42)) }
        )
    }

    @Test
    fun `default extract count is 1`() {
        val converter: MessageConverter<Byte> = MessageConverter.create({ byteArrayOf(it) }, { it[0] }, { "Value$it" }, { "Value$it" })
        assertAll(
            { assertArrayEquals(byteArrayOf(42), converter.toByteArray(42)) },
            { assertEquals(42, converter.fromByteArray(byteArrayOf(42))) },
            { assertEquals("Value42", converter.toTraceString(42)) },
            { assertEquals("Value42", converter.toDebugString(42)) },
            { assertEquals(1, converter.extractCount(42)) }
        )
    }
}
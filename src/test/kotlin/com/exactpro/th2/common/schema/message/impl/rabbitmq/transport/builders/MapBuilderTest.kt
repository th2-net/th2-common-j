/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class MapBuilderTest {
    private val builder = MapBuilder<String, Int>()

    @ParameterizedTest
    @ValueSource(ints = [0, 1, 10])
    fun getSize(elements: Int) {
        repeat(elements) {
            builder.put("key$it", it)
        }
        assertEquals(elements, builder.size, "unexpected size")
    }

    @Test
    fun contains() {
        builder.put("A", 42)

        assertAll(
            {
                assertTrue(builder.contains("A"), "unexpected contains result for existing key")
            },
            {
                assertFalse(builder.contains("B"), "unexpected contains result for not existing key")
            },
        )
    }

    @Test
    fun get() {
        builder.put("A", 42)

        assertAll(
            {
                assertEquals(42, builder["A"], "unexpected value for existing key")
            },
            {
                assertNull(builder["B"], "unexpected value for not existing key")
            }
        )
    }

    @Test
    fun putAll() {
        builder.putAll(mapOf("A" to 42, "B" to 54))

        assertAll(
            {
                assertEquals(42, builder["A"], "unexpected value for existing key A")
            },
            {
                assertEquals(54, builder["B"], "unexpected value for existing key B")
            },
        )
    }

    @Test
    fun remove() {
        builder.put("A", 42)
        builder.put("B", 52)
        builder.remove("A")

        assertAll(
            {
                assertNull(builder["A"], "unexpected value for removed key A")
            },
            {
                assertEquals(52, builder["B"], "unexpected value for remaining key B")
            },
        )
    }

    @Test
    fun removeAll() {
        builder.put("A", 42)
        builder.put("B", 52)
        builder.put("C", 62)

        builder.removeAll("A", "C")

        assertAll(
            {
                assertNull(builder["A"], "unexpected value for removed key A")
            },
            {
                assertNull(builder["C"], "unexpected value for removed key C")
            },
            {
                assertEquals(52, builder["B"], "unexpected value for remaining key B")
            },
        )
    }

    @Test
    fun clear() {
        builder.put("A", 42)
        builder.putAll(mapOf("B" to 52, "C" to 62))

        builder.clear()

        assertAll(
            {
                assertEquals(0, builder.size, "unexpected size after clear")
            },
            {
                assertEquals(emptyMap<String, Int>(), builder.build(), "unexpected map after builder clear")
            },
        )
    }

    @Test
    fun build() {
        builder.put("A", 42)
        builder.putAll(mapOf("B" to 52, "C" to 62))
        builder.remove("B")
        assertEquals(
            mapOf(
                "A" to 42,
                "C" to 62,
            ),
            builder.build(),
            "unexpected build result",
        )
    }
}
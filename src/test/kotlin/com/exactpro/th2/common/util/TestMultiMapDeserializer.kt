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

package com.exactpro.th2.common.util

import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.strategy.route.json.RoutingStrategyModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.commons.collections4.MultiValuedMap
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith

class TestMultiMapDeserializer {

    companion object {
        @JvmStatic
        private val OBJECT_MAPPER: ObjectMapper = ObjectMapper()

        init {
            OBJECT_MAPPER.registerModule(
                KotlinModule.Builder()
                    .withReflectionCacheSize(512)
                    .configure(KotlinFeature.NullToEmptyCollection, false)
                    .configure(KotlinFeature.NullToEmptyMap, false)
                    .configure(KotlinFeature.NullIsSameAsDefault, false)
                    .configure(KotlinFeature.SingletonSupport, true)
                    .configure(KotlinFeature.StrictNullChecks, false)
                    .build()
            )

            OBJECT_MAPPER.registerModule(RoutingStrategyModule(OBJECT_MAPPER))
        }
    }

    @Test
    fun `test negate deserialize`() {
        assertFailsWith(
            MismatchedInputException::class,
        ) {
            OBJECT_MAPPER.readValue("""{"multimap":"string"}""", TestBeanClass::class.java)
        }.also { e ->
            assertEquals("""
                Unexpected token (null), expected START_ARRAY: Can not deserialize MultiValuedMap. Field is not array or object.
                 at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 13] (through reference chain: com.exactpro.th2.common.util.TestMultiMapDeserializer${"$"}TestBeanClass["multimap"])
            """.trimIndent(), e.message)
        }
    }

    @Test
    fun `test positive deserialize`() {
        val bean = OBJECT_MAPPER.readValue("""{"multimap":[{"fieldName":"test", "value":"value", "operation":"EQUAL"},{"fieldName":"test", "value":"value123", "operation":"EQUAL"}]}""", TestBeanClass::class.java)

        val filters = bean.multimap["test"].toList()

        assertEquals(filters.size, 2)
        assertEquals(filters[0].expectedValue, "value")
        assertEquals(filters[1].expectedValue,"value123")
    }

    private data class TestBeanClass(@JsonDeserialize(using = MultiMapFiltersDeserializer::class)  val multimap: MultiValuedMap<String, FieldFilterConfiguration>)

}
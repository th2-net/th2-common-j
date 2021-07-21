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

package com.exactpro.th2.common.util

import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.strategy.route.json.RoutingStrategyModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.commons.collections4.MultiValuedMap
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestMultiMapDeserializer {

    companion object {
        @JvmStatic
        private val OBJECT_MAPPER: ObjectMapper = ObjectMapper()

        init {
            OBJECT_MAPPER.registerModule(KotlinModule())

            OBJECT_MAPPER.registerModule(RoutingStrategyModule(OBJECT_MAPPER))
        }
    }

    @Test
    fun `test negate deserialize`() {
        try {
            OBJECT_MAPPER.readValue<TestBeanClass>("""{"multimap":"string"}""", TestBeanClass::class.java)
        } catch (e: MismatchedInputException) {
            return
        }
        assert(false)
    }

    @Test
    fun `test positive deserialize`() {
        val bean = OBJECT_MAPPER.readValue<TestBeanClass>("""{"multimap":[{"fieldName":"test", "value":"value", "operation":"EQUAL"},{"fieldName":"test", "value":"value123", "operation":"EQUAL"}]}""", TestBeanClass::class.java)

        val filters = bean.multimap["test"].toList()

        assertEquals(filters.size, 2)
        assertEquals(filters[0].expectedValue, "value")
        assertEquals(filters[1].expectedValue,"value123")
    }

    private data class TestBeanClass(@JsonDeserialize(using = MultiMapFiltersDeserializer::class)  val multimap: MultiValuedMap<String, FieldFilterConfiguration>) {}

}
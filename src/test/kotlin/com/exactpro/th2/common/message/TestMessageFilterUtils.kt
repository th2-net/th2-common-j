/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.message

import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.FilterOperation.EQUAL
import com.exactpro.th2.common.grpc.FilterOperation.NOT_EQUAL
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.ValueFilter
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestMessageFilterUtils {

    @Test
    fun `valid tree table conversion`() {
        Assertions.assertNotNull(MessageFilter.newBuilder().apply {
            fillMessage()
            putFields("Message", messageFilter { fillMessage() })
            putFields("MessageCollection", listFilter(
                messageFilter { fillMessage() },
                messageFilter { fillMessage() }
            ))
            putFields("MessageTree", messageFilter {
                putFields("subMessageA", messageFilter {
                    putFields("subMessageB", messageFilter { fillMessage() })
                })
            })

        }.build().toTreeTable())
    }

    private fun MessageFilter.Builder.fillMessage() {
        putFields("KeyString", simpleFilter("key string", NOT_EQUAL, true))
        putFields("NotKeyString", simpleFilter("not key field"))
        putFields("SimpleCollection", listFilter(simpleFilter("A"), simpleFilter("B")))
    }

    private fun simpleFilter(value: String = "", filterOperation: FilterOperation = EQUAL, isKey: Boolean = false) = ValueFilter.newBuilder().apply {
        operation = filterOperation
        simpleFilter = value
        key = isKey
    }.build()

    private fun messageFilter(filterOperation: FilterOperation = EQUAL, isKey: Boolean = false, block: MessageFilter.Builder.() -> Unit) = ValueFilter.newBuilder().apply {
        operation = filterOperation
        key = isKey
        with(messageFilterBuilder) {
            block()
        }
    }.build()

    private fun listFilter(vararg values: ValueFilter) = ValueFilter.newBuilder().apply {
        with(listFilterBuilder) {
            addAllValues(values.toList())
        }
    }.build()
}
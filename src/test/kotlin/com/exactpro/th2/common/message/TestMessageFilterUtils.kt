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
import com.exactpro.th2.common.grpc.MetadataFilter.SimpleFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.ValueFilter
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestMessageFilterUtils {

    private val objectMapper = ObjectMapper()

    //FIXME: Implement converter from RootMessageFilter to JSON
    private val fieldFiltersJson = """"
        |NotKeyString":{"type":"row","columns":{"expected":"not key field","operation":"EQUAL","key":false}},
        |"KeyString":{"type":"row","columns":{"expected":"key string","operation":"NOT_EQUAL","key":true}},
        |"SimpleCollection":{"type":"collection","rows":{
            |"0":{"type":"row","columns":{"expected":"A","operation":"EQUAL","key":false}},
            |"1":{"type":"row","columns":{"expected":"B","operation":"EQUAL","key":false}""".trimMargin().replace("\n", "")
    private val messageFilterBodyJson = """"
        |rows":{
            |"MessageCollection":{"type":"collection","rows":{
                |"0":{"type":"collection","rows":{${fieldFiltersJson}}}}}},
                |"1":{"type":"collection","rows":{${fieldFiltersJson}}}}}}}},
            |"Message":{"type":"collection","rows":{${fieldFiltersJson}}}}}},
            |"MessageTree":{"type":"collection","rows":{
                |"subMessageA":{"type":"collection","rows":{
                    |"subMessageB":{"type":"collection","rows":{${fieldFiltersJson}}}}}}}}}},${fieldFiltersJson}}}}}""".trimMargin().replace("\n", "")
    private val messageFilterJson = """{"type":"treeTable",$messageFilterBodyJson}"""
    private val rootMessageFilterJson = """
        |{"type":"treeTable","rows":{
            |"message-filter":{"type":"collection",$messageFilterBodyJson},
            |"message-type":{"type":"row","columns":{"type":"MsgType"}},
            |"comparison-settings":{"type":"collection","rows":{
                |"ignore-fields":{"type":"collection","rows":{
                    |"0":{"type":"row","columns":{"name":"fieldA"}},
                    |"1":{"type":"row","columns":{"name":"fieldB"}}}}}},
            |"metadata-filter":{"type":"collection","rows":{
                |"propB":{"type":"row","columns":{"expected":"valB","operation":"EQUAL","key":false}},
                |"propA":{"type":"row","columns":{"expected":"valA","operation":"NOT_EQUAL","key":true}}}}}}""".trimMargin().replace("\n", "")

    @Test
    fun `valid message filter to tree table conversion`() {
        val toTreeTable = createMessageFilter().toTreeTable()
        Assertions.assertNotNull(toTreeTable)
        Assertions.assertEquals(messageFilterJson, objectMapper.writeValueAsString(toTreeTable))
    }

    @Test
    fun `valid root message filter to tree table conversion`() {
        val toTreeTable = RootMessageFilter.newBuilder().apply {
            messageType = "MsgType"
            messageFilter = createMessageFilter()
            metadataFilterBuilder.apply {
                putPropertyFilters("propA", simplePropertyFilter("valA", NOT_EQUAL, true))
                putPropertyFilters("propB", simplePropertyFilter("valB"))
            }
            comparisonSettingsBuilder.apply {
                addIgnoreFields("fieldA")
                addIgnoreFields("fieldB")
            }
        }.build().toTreeTable()
        Assertions.assertNotNull(toTreeTable)
        Assertions.assertEquals(rootMessageFilterJson, objectMapper.writeValueAsString(toTreeTable))
    }

    private fun createMessageFilter(): MessageFilter {
        return MessageFilter.newBuilder().apply {
            fillMessage(this)
            putFields("Message", messageFilter { fillMessage(this) })
            putFields("MessageCollection", listFilter(
                messageFilter { fillMessage(this) },
                messageFilter { fillMessage(this) }
            ))
            putFields("MessageTree", messageFilter {
                putFields("subMessageA", messageFilter {
                    putFields("subMessageB", messageFilter { fillMessage(this) })
                })
            })

        }.build()
    }

    private fun fillMessage(builder: MessageFilter.Builder) {
        builder.putFields("KeyString", simpleValueFilter("key string", NOT_EQUAL, true))
            .putFields("NotKeyString", simpleValueFilter("not key field"))
            .putFields("SimpleCollection", listFilter(simpleValueFilter("A"), simpleValueFilter("B")))
    }

    private fun simpleValueFilter(value: String = "", filterOperation: FilterOperation = EQUAL, isKey: Boolean = false) = ValueFilter.newBuilder().apply {
        operation = filterOperation
        simpleFilter = value
        key = isKey
    }.build()

    private fun simplePropertyFilter(value: String = "", filterOperation: FilterOperation = EQUAL, isKey: Boolean = false) = SimpleFilter.newBuilder().apply {
        operation = filterOperation
        this.value = value
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
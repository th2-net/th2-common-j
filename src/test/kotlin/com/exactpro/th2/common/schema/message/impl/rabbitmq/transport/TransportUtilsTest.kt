/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.transport

import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.BOOK_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.DIRECTION_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.MESSAGE_TYPE_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.PROTOCOL_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.SESSION_ALIAS_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.SESSION_GROUP_KEY
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterOperation
import com.exactpro.th2.common.schema.message.configuration.FieldFilterOperation.EQUAL
import com.exactpro.th2.common.schema.message.configuration.FieldFilterOperation.NOT_EMPTY
import com.exactpro.th2.common.schema.message.configuration.FieldFilterOperation.NOT_EQUAL
import com.exactpro.th2.common.schema.message.configuration.FieldFilterOperation.WILDCARD
import com.exactpro.th2.common.schema.message.configuration.MqRouterFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.util.emptyMultiMap
import org.apache.commons.collections4.MultiMapUtils
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import kotlin.test.assertNull
import kotlin.test.assertSame

class TransportUtilsTest {

    private val bookA = "bookA"
    private val bookB = "bookB"

    private val groupA = "groupA"
    private val groupB = "groupB"

    private val msgType = "msg-type"

    private val directionA = "SECOND"
    private val directionB = "FIRST"
    private val protocolA = "protocolA"
    private val protocolB = "protocolB"

    private val routerFilters = listOf(
        MqRouterFilterConfiguration(
            MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                putAll(
                    BOOK_KEY, listOf(
                        FieldFilterConfiguration(BOOK_KEY, bookA, EQUAL),
                        FieldFilterConfiguration(BOOK_KEY, "*A", WILDCARD),
                        FieldFilterConfiguration(BOOK_KEY, null, NOT_EMPTY),
                        FieldFilterConfiguration(BOOK_KEY, bookB, NOT_EQUAL)
                    )
                )
                put(SESSION_GROUP_KEY, FieldFilterConfiguration(SESSION_GROUP_KEY, "*A", WILDCARD))
                put(SESSION_ALIAS_KEY, FieldFilterConfiguration(SESSION_ALIAS_KEY, null, NOT_EMPTY))
                put(MESSAGE_TYPE_KEY, FieldFilterConfiguration(MESSAGE_TYPE_KEY, null, NOT_EMPTY))
                put(DIRECTION_KEY, FieldFilterConfiguration(DIRECTION_KEY, directionB, NOT_EQUAL))
                put(PROTOCOL_KEY, FieldFilterConfiguration(PROTOCOL_KEY, protocolA, EQUAL))
            },
            emptyMultiMap()
        ),
        MqRouterFilterConfiguration(
            MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                putAll(
                    BOOK_KEY, listOf(
                        FieldFilterConfiguration(BOOK_KEY, bookB, EQUAL),
                        FieldFilterConfiguration(BOOK_KEY, "*B", WILDCARD),
                        FieldFilterConfiguration(BOOK_KEY, null, NOT_EMPTY),
                        FieldFilterConfiguration(BOOK_KEY, bookA, NOT_EQUAL)
                    )
                )
                put(SESSION_GROUP_KEY, FieldFilterConfiguration(SESSION_GROUP_KEY, "*B", WILDCARD))
                put(SESSION_ALIAS_KEY, FieldFilterConfiguration(SESSION_ALIAS_KEY, null, NOT_EMPTY))
                put(MESSAGE_TYPE_KEY, FieldFilterConfiguration(MESSAGE_TYPE_KEY, null, NOT_EMPTY))
                put(DIRECTION_KEY, FieldFilterConfiguration(DIRECTION_KEY, directionA, NOT_EQUAL))
                put(PROTOCOL_KEY, FieldFilterConfiguration(PROTOCOL_KEY, protocolB, EQUAL))
            },
            emptyMultiMap()
        )
    )

    @ParameterizedTest
    @ValueSource(strings = ["", "data"])
    fun `empty filter test`(strValue: String) {
        val group = GroupBatch.builder()
            .setBook(strValue)
            .setSessionGroup(strValue)
            .build()
        assertSame(group, listOf<RouterFilter>().filter(group))
    }

    @ParameterizedTest
    @CsvSource(
        "abc,abc,!abc,EQUAL",
        "abc,!abc,abc,NOT_EQUAL",
        "'','',abc,EMPTY",
        "'',abc,'',NOT_EMPTY",
        "abc_*,abc_,_abc_,WILDCARD",
        "abc_*,abc_123,_abc_,WILDCARD",
        "abc_?,abc_1,abc_,WILDCARD",
        "abc_?,abc_a,abc_12,WILDCARD",
        "abc_*,_abc_,abc_,NOT_WILDCARD",
        "abc_*,_abc_,abc_123,NOT_WILDCARD",
        "abc_?,abc_,abc_1,NOT_WILDCARD",
        "abc_?,abc_12,abc_a,NOT_WILDCARD",
    )
    fun `filter operation test`(filterValue: String, validValue: String, invalidValue: String, operation: String) {
        val filters = listOf(
            MqRouterFilterConfiguration(
                MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put(
                        SESSION_ALIAS_KEY,
                        FieldFilterConfiguration(
                            SESSION_ALIAS_KEY,
                            filterValue,
                            FieldFilterOperation.valueOf(operation)
                        )
                    )
                },
                emptyMultiMap()
            )
        )

        val batch = EMPTY_MESSAGE_ID.toBuilder().setSessionAlias(validValue).build().toBatch()
        assertSame(batch, filters.filter(batch), "'$validValue' matched by '$filterValue'[$operation] filter")

        assertNull(filters.filter(
            EMPTY_MESSAGE_ID.toBuilder().setSessionAlias(invalidValue).build().toBatch()
        ), "'$validValue' isn't matched by '$filterValue'[$operation] filter")
    }

    @TestFactory
    fun `filter test`(): Collection<DynamicTest> {
        return listOf(
            DynamicTest.dynamicTest("empty batch") {
                val batch = EMPTY_MESSAGE_ID.toBatch()
                assertNull(routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("only book match") {
                val batch = EMPTY_MESSAGE_ID.toBatch(book = bookA)
                assertNull(routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("only book and group match") {
                val batch = EMPTY_MESSAGE_ID.toBatch(book = bookA, sessionGroup = groupA)
                assertNull(routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("only book, group, protocol match") {
                val batch = EMPTY_MESSAGE_ID
                    .toBatch(book = bookA, sessionGroup = groupA, protocol = protocolA)
                assertNull(routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("with partial message match") {
                val batch = EMPTY_MESSAGE_ID
                    .toBatch(book = bookA, sessionGroup = groupA, type = msgType, protocol = protocolA)
                assertNull(routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("full match for A") {
                val batch = EMPTY_MESSAGE_ID.toBuilder().setSessionAlias("alias").build()
                    .toBatch(book = bookA, sessionGroup = groupA, type = msgType, protocol = protocolA)
                assertSame(batch, routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("full match for B") {
                val batch = EMPTY_MESSAGE_ID.toBuilder()
                    .setSessionAlias("alias").setDirection(Direction.INCOMING).build()
                    .toBatch(book = bookB, sessionGroup = groupB, type = msgType, protocol = protocolB)
                assertSame(batch, routerFilters.filter(batch))
            },
        )
    }

    companion object {
        private val EMPTY_MESSAGE_ID = MessageId.builder()
            .setSessionAlias("")
            .setDirection(Direction.OUTGOING)
            .setSequence(1)
            .setTimestamp(Instant.now())
            .build()

        private fun MessageId.toBatch(
            book: String = "",
            sessionGroup: String = "",
            type: String = "",
            protocol: String = "",
        ) = GroupBatch.builder()
            .setBook(book)
            .setSessionGroup(sessionGroup)
            .addGroup(
                MessageGroup.builder()
                    .addMessage(
                        ParsedMessage.builder()
                            .setId(this)
                            .setType(type)
                            .setProtocol(protocol)
                            .setBody(emptyMap())
                            .build()
                    )
                    .build()
            ).build()
    }
}
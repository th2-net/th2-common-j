/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

    @TestFactory
    fun `filter test`(): Collection<DynamicTest> {
        return listOf(
            DynamicTest.dynamicTest("empty batch") {
                val batch = GroupBatch.builder()
                    .setBook("")
                    .setSessionGroup("")
                    .addGroup(
                        MessageGroup.builder()
                            .addMessage(
                                ParsedMessage.builder()
                                    .setId(
                                        MessageId.builder()
                                            .setSessionAlias("")
                                            .setDirection(Direction.OUTGOING)
                                            .setSequence(1)
                                            .setTimestamp(Instant.now())
                                            .build()
                                    )
                                    .setType("")
                                    .setBody(emptyMap())
                                    .build()
                            )
                            .build()
                    ).build()
                assertNull(routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("only book match") {
                val batch = GroupBatch.builder()
                    .setBook(bookA)
                    .setSessionGroup("")
                    .addGroup(
                        MessageGroup.builder()
                            .addMessage(
                                ParsedMessage.builder()
                                    .setId(
                                        MessageId.builder()
                                            .setSessionAlias("")
                                            .setDirection(Direction.OUTGOING)
                                            .setSequence(1)
                                            .setTimestamp(Instant.now())
                                            .build()
                                    )
                                    .setType("")
                                    .setBody(emptyMap())
                                    .build()
                            )
                            .build()
                    ).build()
                assertNull(routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("only book and group match") {
                val batch = GroupBatch.builder()
                    .setBook(bookA)
                    .setSessionGroup(groupA)
                    .addGroup(
                        MessageGroup.builder()
                            .addMessage(
                                ParsedMessage.builder()
                                    .setId(
                                        MessageId.builder()
                                            .setSessionAlias("")
                                            .setDirection(Direction.OUTGOING)
                                            .setSequence(1)
                                            .setTimestamp(Instant.now())
                                            .build()
                                    )
                                    .setType("")
                                    .setBody(emptyMap())
                                    .build()
                            )
                            .build()
                    ).build()
                assertNull(routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("only book, group, protocol match") {
                val batch = GroupBatch.builder()
                    .setBook(bookA)
                    .setSessionGroup(groupA)
                    .addGroup(
                        MessageGroup.builder()
                            .addMessage(
                                ParsedMessage.builder()
                                    .setProtocol(protocolA)
                                    .setId(
                                        MessageId.builder()
                                            .setSessionAlias("")
                                            .setDirection(Direction.OUTGOING)
                                            .setSequence(1)
                                            .setTimestamp(Instant.now())
                                            .build()
                                    )
                                    .setType("")
                                    .setBody(emptyMap())
                                    .build()
                            )
                            .build()
                    ).build()
                assertNull(routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("with partial message match") {
                val batch = GroupBatch.builder()
                    .setBook(bookA)
                    .setSessionGroup(groupA)
                    .addGroup(
                        MessageGroup.builder()
                            .addMessage(
                                ParsedMessage.builder()
                                    .setProtocol(protocolA)
                                    .setId(
                                        MessageId.builder()
                                            .setSessionAlias("")
                                            .setDirection(Direction.OUTGOING)
                                            .setSequence(1)
                                            .setTimestamp(Instant.now())
                                            .build()
                                    )
                                    .setType(msgType)
                                    .setBody(emptyMap())
                                    .build()
                            )
                            .build()
                    ).build()
                assertNull(routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("full match for A") {
                val batch = GroupBatch.builder()
                    .setBook(bookA)
                    .setSessionGroup(groupA)
                    .addGroup(
                        MessageGroup.builder()
                            .addMessage(
                                ParsedMessage.builder()
                                    .setType(msgType)
                                    .setBody(emptyMap())
                                    .setProtocol(protocolA)
                                    .apply {
                                        idBuilder()
                                            .setSessionAlias("alias")
                                            .setDirection(Direction.OUTGOING)
                                            .setSequence(1)
                                            .setTimestamp(Instant.now())
                                    }.build()
                            )
                            .build()
                    ).build()
                assertSame(batch, routerFilters.filter(batch))
            },
            DynamicTest.dynamicTest("full match for B") {
                val batch = GroupBatch.builder()
                    .setBook(bookB)
                    .setSessionGroup(groupB)
                    .addGroup(
                        MessageGroup.builder()
                            .addMessage(
                                ParsedMessage.builder()
                                    .setType(msgType)
                                    .setBody(emptyMap())
                                    .setProtocol(protocolB)
                                    .apply {
                                        idBuilder()
                                            .setSessionAlias("alias")
                                            .setDirection(Direction.INCOMING)
                                            .setSequence(1)
                                            .setTimestamp(Instant.now())
                                    }.build()
                            )
                            .build()
                    ).build()
                assertSame(batch, routerFilters.filter(batch))
            },
        )
    }
}
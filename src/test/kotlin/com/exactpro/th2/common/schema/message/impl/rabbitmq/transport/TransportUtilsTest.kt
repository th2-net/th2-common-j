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

import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.*
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterOperation.*
import com.exactpro.th2.common.schema.message.configuration.MqRouterFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.util.emptyMultiMap
import org.apache.commons.collections4.MultiMapUtils
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
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

    private val routerFilters = listOf(
        MqRouterFilterConfiguration(
            MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                putAll(BOOK_KEY, listOf(
                    FieldFilterConfiguration(BOOK_KEY, bookA, EQUAL),
                    FieldFilterConfiguration(BOOK_KEY, "*A", WILDCARD),
                    FieldFilterConfiguration(BOOK_KEY, null, NOT_EMPTY),
                    FieldFilterConfiguration(BOOK_KEY, bookB, NOT_EQUAL)
                ))
                put(SESSION_GROUP_KEY, FieldFilterConfiguration(SESSION_GROUP_KEY, "*A", WILDCARD))
                put(SESSION_ALIAS_KEY, FieldFilterConfiguration(SESSION_ALIAS_KEY, null, EMPTY))
                put(MESSAGE_TYPE_KEY, FieldFilterConfiguration(MESSAGE_TYPE_KEY, null, NOT_EMPTY))
                put(DIRECTION_KEY, FieldFilterConfiguration(DIRECTION_KEY, directionB, NOT_EQUAL))
            },
            emptyMultiMap()
        ),
        MqRouterFilterConfiguration(
            MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                putAll(BOOK_KEY, listOf(
                    FieldFilterConfiguration(BOOK_KEY, bookB, EQUAL),
                    FieldFilterConfiguration(BOOK_KEY, "*B", WILDCARD),
                    FieldFilterConfiguration(BOOK_KEY, null, NOT_EMPTY),
                    FieldFilterConfiguration(BOOK_KEY, bookA, NOT_EQUAL)
                ))
                put(SESSION_GROUP_KEY, FieldFilterConfiguration(SESSION_GROUP_KEY, "*B", WILDCARD))
                put(SESSION_ALIAS_KEY, FieldFilterConfiguration(SESSION_ALIAS_KEY, null, EMPTY))
                put(MESSAGE_TYPE_KEY, FieldFilterConfiguration(MESSAGE_TYPE_KEY, null, NOT_EMPTY))
                put(DIRECTION_KEY, FieldFilterConfiguration(DIRECTION_KEY, directionA, NOT_EQUAL))
            },
            emptyMultiMap()
        )
    )

    @Test
    fun `empty filter test`() {
        val group = GroupBatch()
        assertSame(group, listOf<RouterFilter>().filter(group))

        group.book = bookB
        group.sessionGroup = groupA
        assertSame(group, listOf<RouterFilter>().filter(group))

    }

    @Test
    fun `filter test`() {
        val batch = GroupBatch.newMutable()
        assertNull(routerFilters.filter(batch))

        batch.book = bookA
        assertNull(routerFilters.filter(batch))
        batch.sessionGroup = groupA
        assertNull(routerFilters.filter(batch))
        val group = MessageGroup.newMutable()
        batch.groups.add(group)
        assertNull(routerFilters.filter(batch))

        val parsedMessage = ParsedMessage.newMutable()
        group.messages.add(parsedMessage)
        assertNull(routerFilters.filter(batch))

        parsedMessage.type = msgType
        assertNull(routerFilters.filter(batch))

        parsedMessage.id = MessageId(direction = Direction.OUTGOING)
        assertEquals(batch, routerFilters.filter(batch))

        parsedMessage.id = MessageId(direction = Direction.INCOMING)
        batch.sessionGroup = groupB
        batch.book = bookB
        assertEquals(batch, routerFilters.filter(batch))
    }
}
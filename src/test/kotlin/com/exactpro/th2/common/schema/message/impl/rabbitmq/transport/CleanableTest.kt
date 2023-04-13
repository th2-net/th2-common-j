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

import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class CleanableTest {

    private val filledMessageId = MessageId(
        "book",
        "sessionGroup",
        "sessionAlias",
        Direction.OUTGOING,
        1,
        mutableListOf(2, 3),
        Instant.now(),
    )
    private val filledEventId = EventId(
        "id",
        "book",
        "scope",
        Instant.now(),
    )

    @Test
    fun `event id clean test`() {
        val empty = EventId.DEFAULT_INSTANCE

        val mutable = EventId.newMutable().apply {
            id = "id"
            book = "book"
            scope = "scope"
            timestamp = Instant.now()
        }
        assertNotEquals(empty, mutable)

        mutable.clean()
        assertEquals(empty, mutable)
    }

    @Test
    fun `message id clean test`() {
        val empty = MessageId.DEFAULT_INSTANCE

        val mutable = MessageId.newMutable().apply {
            book = "book"
            sessionGroup = "sessionGroup"
            sessionAlias = "sessionAlias"
            direction = Direction.OUTGOING
            sequence = 1
            subsequence = mutableListOf(2, 3)
            timestamp = Instant.now()
        }
        assertNotEquals(empty, mutable)
        
        mutable.clean()
        assertEquals(empty, mutable)
    }

    @Test
    fun `raw message clean test`() {
        val empty = RawMessage()

        val mutable = RawMessage.newMutable().apply {
            id = filledMessageId
            eventId = filledEventId
            metadata["property"] = "value"
            protocol = "protocol"
            body.writeByte(64)
        }
        assertNotEquals(empty, mutable)

        mutable.clean()
        assertEquals(empty, mutable)

        val softMutable = RawMessage.newSoftMutable().apply {
            id = filledMessageId
            eventId = filledEventId
            metadata["property"] = "value"
            protocol = "protocol"
            body = Unpooled.buffer().writeByte(64)
        }
        assertNotEquals(empty, softMutable)

        softMutable.softClean()
        assertEquals(empty, softMutable)
    }

    @Test
    fun `parsed message clean test`() {
        val empty = ParsedMessage()

        val mutable = ParsedMessage.newMutable().apply {
            id = filledMessageId
            eventId = filledEventId
            metadata["property"] = "value"
            protocol = "protocol"
            type = "type"
        }
        assertNotEquals(empty, mutable)

        mutable.clean()
        assertEquals(empty, mutable)

        val softMutable = ParsedMessage.newSoftMutable().apply {
            id = filledMessageId
            eventId = filledEventId
            metadata["property"] = "value"
            protocol = "protocol"
            type = "type"
        }
        assertNotEquals(empty, softMutable)

        softMutable.softClean()
        assertEquals(empty, softMutable)
    }

    @Test
    fun `message group clean test`() {
        val empty = MessageGroup()

        val mutable = MessageGroup.newMutable().apply {
            messages.add(RawMessage())
        }
        assertNotEquals(empty, mutable)

        mutable.clean()
        assertEquals(empty, mutable)

        mutable.messages.add(ParsedMessage())
        assertNotEquals(empty, mutable)

        mutable.softClean()
        assertEquals(empty, mutable)
    }

    @Test
    fun `group batch clean test`() {
        val empty = GroupBatch()

        val mutable = GroupBatch.newMutable().apply {
            groups.add(MessageGroup())
        }
        assertNotEquals(empty, mutable)

        mutable.clean()
        assertEquals(empty, mutable)

        mutable.groups.add(MessageGroup())
        assertNotEquals(empty, mutable)

        mutable.softClean()
        assertEquals(empty, mutable)
    }
}
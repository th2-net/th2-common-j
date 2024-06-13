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

import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.assertAll
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor

class CodecsTest {

    @Test
    fun `decode encode test`() {
        val buffer = Unpooled.buffer()

        val message1 = RawMessage(
            id = MessageId(
                sessionAlias = "alias1",
                direction = Direction.INCOMING,
                sequence = 1,
                subsequence = mutableListOf(1, 2),
                timestamp = Instant.now()
            ),
            metadata = mutableMapOf(
                "prop1" to "value1",
                "prop2" to "value2"
            ),
            protocol = "proto1",
            body = Unpooled.wrappedBuffer(byteArrayOf(1, 2, 3, 4))
        )

        val message2 = RawMessage(
            id = MessageId(
                sessionAlias = "alias2",
                direction = Direction.OUTGOING,
                sequence = 2,
                subsequence = mutableListOf(3, 4),
                timestamp = Instant.now()
            ),
            metadata = mutableMapOf(
                "prop3" to "value3",
                "prop4" to "value4"
            ),
            protocol = "proto2",
            body = Unpooled.wrappedBuffer(byteArrayOf(5, 6, 7, 8))
        )

        val message3 = ParsedMessage(
            id = MessageId(
                sessionAlias = "alias3",
                direction = Direction.OUTGOING,
                sequence = 3,
                subsequence = mutableListOf(5, 6),
                timestamp = Instant.now()
            ),
            metadata = mutableMapOf(
                "prop5" to "value6",
                "prop7" to "value8"
            ),
            protocol = "proto3",
            type = "some-type",
            rawBody = Unpooled.buffer().apply { writeCharSequence("{}", Charsets.UTF_8) }
        )

        val batch = GroupBatch(
            book = "book1",
            sessionGroup = "group1",
            groups = mutableListOf(MessageGroup(mutableListOf(message1, message2, message3)))
        )

        GroupBatchCodec.encode(batch, buffer)
        val decodedBatch = GroupBatchCodec.decode(buffer)

        assertEquals(batch, decodedBatch)
    }

    @Test
    fun `raw body is updated in parsed message when body is changed`() {
        val parsedMessage = ParsedMessage.builder().apply {
            idBuilder()
                .setSessionAlias("alias1")
                .setDirection(Direction.INCOMING)
                .setSequence(1)
                .addSubsequence(1)
                .setTimestamp(Instant.now())
            setType("test")
            setBody(
                linkedMapOf(
                    "field" to 42,
                    "another" to "test_data",
                )
            )
        }.build()

        val dest = Unpooled.buffer()
        ParsedMessageCodec.encode(parsedMessage, dest)
        val decoded = ParsedMessageCodec.decode(DecodeContext.create(), dest)
        assertEquals(0, dest.readableBytes()) { "unexpected bytes left: ${ByteBufUtil.hexDump(dest)}" }

        assertEquals(parsedMessage, decoded, "unexpected parsed result decoded")
        assertEquals(
            Unpooled.buffer().apply {
                writeCharSequence("{\"field\":42,\"another\":\"test_data\"}", Charsets.UTF_8)
            },
            decoded.rawBody,
            "unexpected raw body",
        )
    }

    @Test
    fun `book and session group from batch are available on message id level`() {
        val message1 = RawMessage(
            id = MessageId(
                sessionAlias = "alias2",
                direction = Direction.OUTGOING,
                sequence = 2,
                subsequence = mutableListOf(3, 4),
                timestamp = Instant.now()
            ),
            metadata = mutableMapOf(
                "prop3" to "value3",
                "prop4" to "value4"
            ),
            protocol = "proto2",
            body = Unpooled.wrappedBuffer(byteArrayOf(5, 6, 7, 8))
        )

        val message2 = ParsedMessage(
            id = MessageId(
                sessionAlias = "alias3",
                direction = Direction.OUTGOING,
                sequence = 3,
                subsequence = mutableListOf(5, 6),
                timestamp = Instant.now()
            ),
            metadata = mutableMapOf(
                "prop5" to "value6",
                "prop7" to "value8"
            ),
            protocol = "proto3",
            type = "some-type",
            rawBody = Unpooled.buffer().apply { writeCharSequence("{}", Charsets.UTF_8) }
        )

        val batch = GroupBatch(
            book = "book1",
            sessionGroup = "group1",
            groups = mutableListOf(MessageGroup(mutableListOf(message1, message2)))
        )
        val target = Unpooled.buffer()
        GroupBatchCodec.encode(batch, target)
        val decoded = GroupBatchCodec.decode(target)

        val messages = decoded.groups.flatMap { it.messages }
        assertEquals(2, messages.size)

        assertAll(
            messages.map {
                {
                    assertAll(
                        heading = "${it::class} has book and session group",
                        {
                            assertEquals("book1", it.id.book, "unexpected book")
                        },
                        {
                            assertEquals("group1", it.id.sessionGroup, "unexpected session group")
                        },
                    )
                }
            }
        )
    }

    @TestFactory
    fun dateTypesTests(): Collection<DynamicTest> {
        LocalTime.parse("16:36:38.035420").toString()
        val testData = listOf<Pair<TemporalAccessor, (TemporalAccessor) -> String>>(
            LocalDate.now() to TemporalAccessor::toString,
            LocalTime.now() to DateTimeFormatter.ISO_LOCAL_TIME::format,
            // Check case when LocalTime.toString() around nanos to 1000
            LocalTime.parse("16:36:38.035420") to DateTimeFormatter.ISO_LOCAL_TIME::format,
            LocalDateTime.now() to DateTimeFormatter.ISO_LOCAL_DATE_TIME::format,
            Instant.now() to TemporalAccessor::toString,
        )
        return testData.map { (value, formatter) ->
            DynamicTest.dynamicTest("serializes ${value::class.simpleName} as field") {
                val parsedMessage = ParsedMessage.builder().apply {
                    setId(MessageId.DEFAULT)
                    setType("test")
                    setBody(
                        linkedMapOf(
                            "field" to value,
                        )
                    )
                }.build()

                val dest = Unpooled.buffer()
                ParsedMessageCodec.encode(parsedMessage, dest)
                val decoded = ParsedMessageCodec.decode(DecodeContext.create(), dest)
                assertEquals(0, dest.readableBytes()) { "unexpected bytes left: ${ByteBufUtil.hexDump(dest)}" }

                assertEquals(parsedMessage, decoded, "unexpected parsed result decoded")
                assertEquals(
                    "{\"field\":\"${formatter(value)}\"}",
                    decoded.rawBody.toString(Charsets.UTF_8),
                    "unexpected raw body",
                )
            }
        }
    }
}
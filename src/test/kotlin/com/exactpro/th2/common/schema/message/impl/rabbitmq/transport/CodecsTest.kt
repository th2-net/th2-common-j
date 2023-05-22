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

import com.fasterxml.jackson.module.kotlin.readValue
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant

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
        }.build { buf -> ByteBufInputStream(buf).use { ParsedMessageCodec.MAPPER.readValue(it) } }

        val dest = Unpooled.buffer()
        ParsedMessageCodec.encode(parsedMessage, dest)
        val decoded = ParsedMessageCodec.decode(dest)
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
}
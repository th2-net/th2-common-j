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

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import java.time.Instant
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ParsedMessageTest {

    private val testBody = hashMapOf("test-field" to "test-filed-value")
    private val testMetadata = hashMapOf("test-property" to "test-property-value")
    private val testTimestamp = Instant.now()

    @Test
    fun `builder test`() {
        val builder = ParsedMessage.builder().apply {
            setType(TEST_MESSAGE_TYPE)
            setProtocol(TEST_PROTOCOL)
            setBody(testBody)
            setMetadata(testMetadata)
            idBuilder().apply {
                setSessionAlias(TEST_SESSION_ALIAS)
                setDirection(Direction.OUTGOING)
                setSequence(TEST_SEQUENCE)
                setSubsequence(TEST_SUB_SEQUENCE)
                setTimestamp(testTimestamp)
            }
            setEventId(EventId.builder().apply {
                setBook(TEST_BOOK)
                setScope(TEST_SCOPE)
                setTimestamp(testTimestamp)
                setId(TEST_EVENT_ID)
            }.build())
        }

        with(builder) {
            assertEquals(TEST_MESSAGE_TYPE, type)
            assertEquals(TEST_PROTOCOL, protocol)
            with(bodyBuilder()) {
                assertEquals(testBody.size, size)
                assertAll(testBody.map { (key, value) ->
                    { assertEquals(value, get(key), "Check '$key' field") }
                })
            }
            with(metadataBuilder()) {
                assertEquals(testMetadata.size, size)
                assertAll(testMetadata.map { (key, value) ->
                    { assertEquals(value, get(key), "Check '$key' field") }
                })
            }
            with(idBuilder()) {
                assertEquals(TEST_SESSION_ALIAS, sessionAlias)
                assertEquals(Direction.OUTGOING, direction)
                assertEquals(TEST_SEQUENCE, sequence)
                with(subsequenceBuilder()) {
                    assertEquals(TEST_SUB_SEQUENCE.size, size)
                    assertAll(TEST_SUB_SEQUENCE.mapIndexed { index, value ->
                        { assertEquals(value, get(index), "Check '$index' index") }
                    })
                }
                assertEquals(testTimestamp, timestamp)
            }
            with(assertNotNull(eventId)) {
                assertEquals(TEST_BOOK, book)
                assertEquals(TEST_SCOPE, scope)
                assertEquals(testTimestamp, testTimestamp)
                assertEquals(TEST_EVENT_ID, id)
            }
        }

        with(builder.build()) {
            assertEquals(TEST_MESSAGE_TYPE, type)
            assertEquals(TEST_PROTOCOL, protocol)
            assertEquals(testBody, body)
            assertEquals(testMetadata, metadata)
            with(id) {
                assertEquals(TEST_SESSION_ALIAS, sessionAlias)
                assertEquals(Direction.OUTGOING, direction)
                assertEquals(TEST_SEQUENCE, sequence)
                assertEquals(TEST_SUB_SEQUENCE, subsequence)
                assertEquals(testTimestamp, timestamp)
            }
            with(assertNotNull(eventId)) {
                assertEquals(TEST_BOOK, book)
                assertEquals(TEST_SCOPE, scope)
                assertEquals(testTimestamp, testTimestamp)
                assertEquals(TEST_EVENT_ID, id)
            }
        }
    }

    @Test
    fun `toBuilder test`() {
        val message = ParsedMessage.builder().apply {
            setType(TEST_MESSAGE_TYPE)
            setProtocol(TEST_PROTOCOL)
            setBody(testBody)
            setMetadata(testMetadata)
            idBuilder().apply {
                setSessionAlias(TEST_SESSION_ALIAS)
                setDirection(Direction.OUTGOING)
                setSequence(TEST_SEQUENCE)
                setSubsequence(TEST_SUB_SEQUENCE)
                setTimestamp(testTimestamp)
            }
            setEventId(EventId.builder().apply {
                setBook(TEST_BOOK)
                setScope(TEST_SCOPE)
                setTimestamp(testTimestamp)
                setId(TEST_EVENT_ID)
            }.build())
        }.build()

        val builder = message.toBuilder()
        with(builder) {
            assertEquals(TEST_MESSAGE_TYPE, type)
            assertEquals(TEST_PROTOCOL, protocol)
            with(bodyBuilder()) {
                assertEquals(testBody.size, size)
                assertAll(testBody.map { (key, value) ->
                    { assertEquals(value, get(key), "Check '$key' field") }
                })
            }
            with(metadataBuilder()) {
                assertEquals(testMetadata.size, size)
                assertAll(testMetadata.map { (key, value) ->
                    { assertEquals(value, get(key), "Check '$key' field") }
                })
            }
            with(idBuilder()) {
                assertEquals(TEST_SESSION_ALIAS, sessionAlias)
                assertEquals(Direction.OUTGOING, direction)
                assertEquals(TEST_SEQUENCE, sequence)
                with(subsequenceBuilder()) {
                    assertEquals(TEST_SUB_SEQUENCE.size, size)
                    assertAll(TEST_SUB_SEQUENCE.mapIndexed { index, value ->
                        { assertEquals(value, get(index), "Check '$index' index") }
                    })
                }
                assertEquals(testTimestamp, timestamp)
            }
            with(assertNotNull(eventId)) {
                assertEquals(TEST_BOOK, book)
                assertEquals(TEST_SCOPE, scope)
                assertEquals(testTimestamp, testTimestamp)
                assertEquals(TEST_EVENT_ID, id)
            }
        }

        with(builder.build()) {
            assertEquals(TEST_MESSAGE_TYPE, type)
            assertEquals(TEST_PROTOCOL, protocol)
            assertEquals(testBody, body)
            assertEquals(testMetadata, metadata)
            with(id) {
                assertEquals(TEST_SESSION_ALIAS, sessionAlias)
                assertEquals(Direction.OUTGOING, direction)
                assertEquals(TEST_SEQUENCE, sequence)
                assertEquals(TEST_SUB_SEQUENCE, subsequence)
                assertEquals(testTimestamp, timestamp)
            }
            with(assertNotNull(eventId)) {
                assertEquals(TEST_BOOK, book)
                assertEquals(TEST_SCOPE, scope)
                assertEquals(testTimestamp, testTimestamp)
                assertEquals(TEST_EVENT_ID, id)
            }
        }
    }

    companion object {
        private const val TEST_BOOK = "test-book"
        private const val TEST_SCOPE = "test-scope"
        private const val TEST_EVENT_ID = "test-event-id"
        private const val TEST_PROTOCOL = "test-protocol"
        private const val TEST_SESSION_ALIAS = "test-session-alias"
        private const val TEST_MESSAGE_TYPE = "test-message-type"
        private val TEST_SEQUENCE = Random.nextLong()
        private val TEST_SUB_SEQUENCE = listOf(Random.nextInt(), Random.nextInt())
    }
}
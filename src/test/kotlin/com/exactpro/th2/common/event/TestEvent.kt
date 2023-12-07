/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.event

import com.exactpro.th2.common.event.Event.UNKNOWN_EVENT_NAME
import com.exactpro.th2.common.event.Event.UNKNOWN_EVENT_TYPE
import com.exactpro.th2.common.event.EventUtils.toEventID
import com.exactpro.th2.common.event.bean.BaseTest.BOOK_NAME
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus.FAILED
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toTimestamp
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.assertAll
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset

typealias ProtoEvent = com.exactpro.th2.common.grpc.Event

class TestEvent {
    private val parentEventId: EventID = toEventID(Instant.now(), BOOK_NAME, "parentEventId")
    private val data = EventUtils.createMessageBean("0123456789".repeat(20))
    private val dataSize = MAPPER.writeValueAsBytes(listOf(data)).size
    private val bigData = EventUtils.createMessageBean("0123456789".repeat(30))

    @Test
    fun `call the toProto method on a simple event`() {
        Event.start().toProto(BOOK_NAME).run {
            checkDefaultEventFields()
            assertFalse(hasParentId())
        }

        Event.start().toProto(parentEventId).run {
            checkDefaultEventFields()
            assertEquals(parentEventId, parentId)
        }
    }

    @Test
    fun `set parent to the toListProto method`() {
        val event = Event.start()
        val toListProtoWithParent = event.toListProto(parentEventId)
        val toListProtoWithoutParent = event.toListProto(BOOK_NAME)
        assertAll(
            { assertEquals(1, toListProtoWithParent.size) },
            { assertEquals(1, toListProtoWithoutParent.size) },
            { assertEquals(parentEventId, toListProtoWithParent[0].parentId) },
            { assertFalse(toListProtoWithoutParent[0].hasParentId()) }
        )
    }

    @Test
    fun `negative or zero max size`() {
        val rootEvent = Event.start()
        assertAll(
            { assertThrows(IllegalArgumentException::class.java) { rootEvent.toBatchesProtoWithLimit(-1, parentEventId) } },
            { assertThrows(IllegalArgumentException::class.java) { rootEvent.toBatchesProtoWithLimit(0, parentEventId) } }
        )
    }

    @Test
    fun `too low max size`() {
        val rootEvent = Event.start()
            .bodyData(data)

        assertAll(
            { assertThrows(IllegalStateException::class.java) { rootEvent.toBatchesProtoWithLimit(1, parentEventId) } }
        )
    }

    @Test
    fun `every event to distinct batch`() {
        val rootEvent = Event.start()
            .bodyData(data).apply {
                addSubEventWithSamePeriod()
                    .bodyData(data)
                    .addSubEventWithSamePeriod()
                    .bodyData(data)
            }

        val batches = rootEvent.toBatchesProtoWithLimit(dataSize, parentEventId)
        assertEquals(3, batches.size)
        checkEventStatus(batches, 3, 0)
    }

    @Test
    fun `problem events`() {
        val rootEvent = Event.start()
            .bodyData(data).apply {
                addSubEventWithSamePeriod()
                    .bodyData(data)
                    .addSubEventWithSamePeriod()
                    .bodyData(bigData)
            }

        val batches = rootEvent.toBatchesProtoWithLimit(dataSize, parentEventId)
        assertEquals(3, batches.size)
        checkEventStatus(batches, 2, 1)
    }

    @Test
    fun `several events at the end of hierarchy`() {
        val rootEvent = Event.start()
            .bodyData(data).apply {
                addSubEventWithSamePeriod()
                    .bodyData(data)
                addSubEventWithSamePeriod()
                    .bodyData(bigData)
                addSubEventWithSamePeriod()
                    .bodyData(data)
                addSubEventWithSamePeriod()
                    .bodyData(data)
            }

        assertAll(
            {
                val batches = rootEvent.toBatchesProtoWithLimit(dataSize, parentEventId)
                assertEquals(5, batches.size)
                checkEventStatus(batches, 4, 1)
            }, {
                val batches = rootEvent.toBatchesProtoWithLimit(dataSize * 2, parentEventId)
                assertEquals(4, batches.size)
                checkEventStatus(batches, 5, 0)
            }, {
                val batches = rootEvent.toBatchesProtoWithLimit(dataSize * 3, parentEventId)
                assertEquals(3, batches.size)
                checkEventStatus(batches, 5, 0)
            }
        )
    }

    @Test
    fun `batch structure`() {
        val rootEvent = Event.start()
            .bodyData(data)
        val subEvent1 = rootEvent.addSubEventWithSamePeriod()
            .bodyData(data)
        val subEvent2 = rootEvent.addSubEventWithSamePeriod()
            .bodyData(data)

        val batches = rootEvent.toBatchesProtoWithLimit(1024 * 1024, parentEventId)
        assertEquals(2, batches.size)
        checkEventStatus(batches, 3, 0)

        assertFalse(batches[0].hasParentEventId())
        assertEquals(parentEventId, batches[0].eventsList[0].parentId)
        assertEquals(rootEvent.id, batches[0].eventsList[0].id.id)

        assertEquals(rootEvent.id, batches[1].parentEventId.id)
        assertEquals(rootEvent.id, batches[1].eventsList[0].parentId.id)
        assertEquals(subEvent1.id, batches[1].eventsList[0].id.id)
        assertEquals(rootEvent.id, batches[1].eventsList[1].parentId.id)
        assertEquals(subEvent2.id, batches[1].eventsList[1].id.id)
    }

    @Test
    fun `event with children is after the event without children`() {
        val rootEvent = Event.start()
            .bodyData(data).apply {
                addSubEventWithSamePeriod()
                    .bodyData(data)
                addSubEventWithSamePeriod()
                    .bodyData(data).apply {
                        addSubEventWithSamePeriod()
                            .bodyData(data)
                    }
            }

        val batches = rootEvent.toBatchesProtoWithLimit(dataSize, parentEventId)
        assertEquals(4, batches.size)
        checkEventStatus(batches, 4, 0)
    }

    @Test
    fun `root event to list batch proto with size limit`() {
        val rootName = "root"
        val childName = "child"
        val rootEvent = Event.start().also {
            it.name = rootName
            it.bodyData(data).apply {
                addSubEventWithSamePeriod().also { subEvent ->
                    subEvent.name = childName
                    subEvent.bodyData(data)
                }
            }
        }

        val batches = rootEvent.toBatchesProtoWithLimit(dataSize, BOOK_NAME)
        assertEquals(2, batches.size)
        checkEventStatus(batches, 2, 0)

        batches[0].checkEventBatch(false, listOf(rootName))
        batches[1].checkEventBatch(true, listOf(childName))
    }

    @Test
    fun `root event to list batch proto without size limit`() {
        val rootName = "root"
        val childName = "child"
        val rootEvent = Event.start().also {
            it.name = rootName
            it.bodyData(data).apply {
                addSubEventWithSamePeriod().also { subEvent ->
                    subEvent.name = childName
                    subEvent.bodyData(data)
                }
            }
        }

        val batch = rootEvent.toBatchProto(BOOK_NAME)
        checkEventStatus(listOf(batch), 2, 0)
        batch.checkEventBatch(false, listOf(rootName, childName))
    }

    @Test
    fun `event with children is before the event without children`() {
        val rootEvent = Event.start()
            .bodyData(data).apply {
                addSubEventWithSamePeriod()
                    .bodyData(data).apply {
                        addSubEventWithSamePeriod()
                            .bodyData(data)
                    }
                addSubEventWithSamePeriod()
                    .bodyData(data)
            }

        val batches = rootEvent.toBatchesProtoWithLimit(dataSize, parentEventId)
        assertEquals(4, batches.size)
        checkEventStatus(batches, 4, 0)
    }

    @Test
    fun `pack event tree to single batch`() {
        val rootEvent = Event.start()
            .bodyData(data).apply {
                addSubEventWithSamePeriod()
                    .bodyData(data).apply {
                        addSubEventWithSamePeriod()
                            .bodyData(data)
                    }
                addSubEventWithSamePeriod()
                    .bodyData(data)
            }

        val batch = rootEvent.toBatchProto(parentEventId)
        assertEquals(parentEventId, batch.parentEventId)
        checkEventStatus(listOf(batch), 4, 0)
    }

    @Test
    fun `pack single event single batch`() {
        val batch = Event.start().toBatchProto(parentEventId)
        assertFalse(batch.hasParentEventId())
        checkEventStatus(listOf(batch), 1, 0)
    }

    @Test
    fun `serializes date time fields`() {
        class TestBody(
            val instant: Instant,
            val dateTime: LocalDateTime,
            val date: LocalDate,
            val time: LocalTime,
        ) : IBodyData

        // Friday, 13 October 2023 y., 12:35:05
        val instant = Instant.ofEpochSecond(1697200505)
        val protoEvent = Event.start().endTimestamp()
            .bodyData(
                TestBody(
                    instant = instant,
                    dateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC),
                    date = LocalDate.ofInstant(instant, ZoneOffset.UTC),
                    time = LocalTime.ofInstant(instant, ZoneOffset.UTC),
                )
            ).toProto(parentEventId)
        val jsonBody = protoEvent.body.toStringUtf8()
        assertEquals(
            """[{"instant":"2023-10-13T12:35:05Z","dateTime":"2023-10-13T12:35:05","date":"2023-10-13","time":"12:35:05"}]""",
            jsonBody,
            "unexpected JSON body",
        )
    }

    @TestFactory
    fun `book mismatch between attached message and event`(): Collection<DynamicTest> {
        val event = Event.start()
            .messageID(MessageID.newBuilder().apply {
                this.bookName = "${parentEventId.bookName}-test"
                this.connectionIdBuilder.apply {
                    sessionGroup = "test-session-group"
                    sessionAlias = "test-session-alias"
                }
                this.timestamp = Instant.now().toTimestamp()
                this.direction = Direction.SECOND
                this.sequence = 2
            }.build())
        return listOf(
            DynamicTest.dynamicTest("different book in message 'toBatchProto(book)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toBatchProto(parentEventId.bookName)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toBatchProto(book, scope)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toBatchProto(parentEventId.bookName, parentEventId.scope)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toBatchProto(eventId)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toBatchProto(parentEventId)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toBatchesProtoWithLimit(contentSize, book)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toBatchesProtoWithLimit(1, parentEventId.bookName)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toBatchesProtoWithLimit(contentSize, book, scope)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toBatchesProtoWithLimit(1, parentEventId.bookName, parentEventId.scope)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toBatchesProtoWithLimit(contentSize, eventId)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toBatchesProtoWithLimit(1, parentEventId)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toListBatchProto(book)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toListBatchProto(parentEventId.bookName)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toListBatchProto(book, scope)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toListBatchProto(parentEventId.bookName, parentEventId.scope)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toListBatchProto(eventId)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toListBatchProto(parentEventId)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toListProto(book)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toListProto(parentEventId.bookName)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toListProto(book, scope)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toListProto(parentEventId.bookName, parentEventId.scope)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toListProto(eventId)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toListProto(parentEventId)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toProto(book)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toProto(parentEventId.bookName)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toProto(book, scope)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toProto(parentEventId.bookName, parentEventId.scope)
                }
            },
            DynamicTest.dynamicTest("different book in message 'toProto(eventId)'") {
                assertThrows(IllegalArgumentException::class.java) {
                    event.toProto(parentEventId)
                }
            },
        )
    }

    private fun com.exactpro.th2.common.grpc.Event.checkDefaultEventFields() {
        assertAll(
            { assertTrue(hasId()) },
            { assertEquals(UNKNOWN_EVENT_NAME, name) },
            { assertEquals(UNKNOWN_EVENT_TYPE, type) },
            { assertTrue(hasEndTimestamp()) },
            { assertEquals(SUCCESS, status) },
            { assertEquals(ByteString.copyFrom("[]".toByteArray()), body) },
            { assertEquals(0, attachedMessageIdsCount) }
        )
    }

    private fun EventBatch.checkEventBatch(hasParentId: Boolean, eventNames: List<String>) {
        assertEquals(hasParentId, hasParentEventId())
        assertEquals(eventNames.size, eventsCount)
        assertEquals(eventNames, eventsList.map(ProtoEvent::getName).toList())
    }

    private fun checkEventStatus(batches: List<EventBatch>, successNumber: Int, filedNumber: Int) {
        val events = batches.flatMap(EventBatch::getEventsList)
        assertAll(
            { assertEquals(filedNumber + successNumber, events.size, "number") },
            { assertEquals(filedNumber, events.filter { it.status == FAILED }.size, "success") },
            { assertEquals(successNumber, events.filter { it.status == SUCCESS }.size, "failed") }
        )
    }

    companion object {
        private val MAPPER = ObjectMapper()
    }
}
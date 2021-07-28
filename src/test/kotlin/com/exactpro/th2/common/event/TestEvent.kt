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

import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus.FAILED
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

typealias ProtoEvent = com.exactpro.th2.common.grpc.Event

class TestEvent {

    private val parentEventId: EventID = EventUtils.toEventID("parentEventId")!!
    private val data = EventUtils.createMessageBean("0123456789".repeat(20))
    private val dataSize = MAPPER.writeValueAsBytes(listOf(data)).size
    private val bigData = EventUtils.createMessageBean("0123456789".repeat(30))

    @Test
    fun `set parent to the toProto method`() {
        val event = Event.start()
        val parentEventId = EventID.newBuilder().apply {
            id = "test"
        }.build()

        assertAll(
            { assertEquals(parentEventId, event.toProto(parentEventId).parentId) },
            { assertFalse(event.toProto(null).hasParentId()) }
        )
    }

    @Test
    fun `set parent to the toListProto method`() {
        val event = Event.start()
        val parentEventId = EventID.newBuilder().apply {
            id = "test"
        }.build()

        val toListProtoWithParent = event.toListProto(parentEventId)
        val toListProtoWithoutParent = event.toListProto(null)
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
            { Assertions.assertThrows(IllegalArgumentException::class.java) { rootEvent.toListBatchProto(-1, parentEventId) } },
            { Assertions.assertThrows(IllegalArgumentException::class.java) { rootEvent.toListBatchProto(0, parentEventId) } }
        )
    }

    @Test
    fun `too low max size`() {
        val rootEvent = Event.start()
            .bodyData(data)

        assertAll(
            { Assertions.assertThrows(IllegalStateException::class.java) { rootEvent.toListBatchProto(1, parentEventId) } }
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

        val batches = rootEvent.toListBatchProto(dataSize, parentEventId)
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

        val batches = rootEvent.toListBatchProto(dataSize, parentEventId)
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
                val batches = rootEvent.toListBatchProto(dataSize, parentEventId)
                assertEquals(5, batches.size)
                checkEventStatus(batches, 4, 1)
            }, {
                val batches = rootEvent.toListBatchProto(dataSize * 2, parentEventId)
                assertEquals(4, batches.size)
                checkEventStatus(batches, 5, 0)
            }, {
                val batches = rootEvent.toListBatchProto(dataSize * 3, parentEventId)
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

        val batches = rootEvent.toListBatchProto(1024 * 1024, parentEventId)
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

        val batches = rootEvent.toListBatchProto(dataSize, parentEventId)
        assertEquals(4, batches.size)
        checkEventStatus(batches, 4, 0)
    }

    @Test
    fun `root event to list batch proto with size limit`() {
        val rootName = "root"
        val childName = "child"
        val rootEvent = Event.start().apply {
            name = rootName
            bodyData(data).apply {
                addSubEventWithSamePeriod().apply {
                    name = childName
                    bodyData(data)
                }
            }
        }

        val batches = rootEvent.toListBatchProto(dataSize, null)
        assertEquals(2, batches.size)
        checkEventStatus(batches, 2, 0)

        batches[0].checkEventBatch(false, listOf(rootName))
        batches[1].checkEventBatch(true, listOf(childName))
    }

    @Test
    fun `root event to list batch proto without size limit`() {
        val rootName = "root"
        val childName = "child"
        val rootEvent = Event.start().apply {
            name = rootName
            bodyData(data).apply {
                addSubEventWithSamePeriod().apply {
                    name = childName
                    bodyData(data)
                }
            }
        }

        val batch = rootEvent.toBatchProto(null)
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

        val batches = rootEvent.toListBatchProto(dataSize, parentEventId)
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
        val rootEvent = Event.start()

        val batch = rootEvent.toBatchProto(parentEventId)
        assertFalse(batch.hasParentEventId())
        checkEventStatus(listOf(batch), 1, 0)
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
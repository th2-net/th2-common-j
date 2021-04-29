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

import com.exactpro.th2.common.grpc.EventID
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

class TestEvent {

    @Test
    fun `set parent to the toProtoEvent message`() {
        val event = Event.start()
        val parentEventId = EventID.newBuilder().apply {
            id = "test"
        }.build()

        assertAll(
            { assertEquals(parentEventId, event.toProtoEvent(parentEventId).parentId) },
            { assertFalse(event.toProtoEvent(null as EventID?).hasParentId()) }
        )
    }
}
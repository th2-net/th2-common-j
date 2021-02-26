/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.schema.message.impl.rabbitmq.custom
import org.junit.jupiter.api.Assertions.*
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.message.get
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.set
import org.junit.jupiter.api.Test

const val MESSAGE_TYPE_VALUE = "test message type"
const val SESSION_ALIAS_VALUE = "test session alias"
const val FIELD_NAME = "test field"
const val FIELD_VALUE = "test value"
val DIRECTION_VALUE = SECOND

class TestMessageUtil {
    @Test
    fun `fill message type`() {
        message().build().also {
            assertFalse(it.hasMetadata())
        }
        message(MESSAGE_TYPE_VALUE).build().also {
            assertTrue(it.hasMetadata())
            assertEquals(MESSAGE_TYPE_VALUE, it.metadata.messageType)
        }
        message(MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE).build().also {
            assertTrue(it.hasMetadata())
            val metadata = it.metadata
            assertEquals(MESSAGE_TYPE_VALUE, metadata.messageType)
            assertTrue(metadata.hasId())
            val id = metadata.id
            assertEquals(DIRECTION_VALUE, id.direction)
            assertTrue(id.hasConnectionId())
            val connectionId = id.connectionId
            assertEquals(SESSION_ALIAS_VALUE, connectionId.sessionAlias)
        }
    }

    @Test
    fun `set and get operators`() {
        val builder = message()
        builder[FIELD_NAME] = FIELD_VALUE

        builder[FIELD_NAME].also {
            assertNotNull(it)
            assertEquals(FIELD_VALUE, it?.simpleValue)
        }

        builder.build()[FIELD_NAME].also {
            assertNotNull(it)
            assertEquals(FIELD_VALUE, it?.simpleValue)
        }
    }
}

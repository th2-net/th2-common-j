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
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.get
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.set
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
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
            assertEquals(MESSAGE_TYPE_VALUE, it.messageType)
            assertEquals(DIRECTION_VALUE, it.direction)
            assertEquals(SESSION_ALIAS_VALUE, it.sessionAlias)
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

    @Test
    fun `update message type`() {
        val newMessageType = "Hello"
        message(MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE).also {
            assertNotEquals(newMessageType, it.messageType)
        }.apply {
            messageType = newMessageType
        }.also {
            assertEquals(newMessageType, it.messageType)
            assertEquals(DIRECTION_VALUE, it.direction)
            assertEquals(SESSION_ALIAS_VALUE, it.sessionAlias)
        }
    }

    @Test
    fun `update direction`() {
        val newDirection = FIRST
        message(MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE).also {
            assertNotEquals(newDirection, it.direction)
        }.apply {
            direction = newDirection
        }.also {
            assertEquals(MESSAGE_TYPE_VALUE, it.messageType)
            assertEquals(newDirection, it.direction)
            assertEquals(SESSION_ALIAS_VALUE, it.sessionAlias)
        }
    }

    @Test
    fun `update session alias`() {
        val newSessionAlias = "Hello"
        message(MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE).also {
            assertNotEquals(newSessionAlias, it.sessionAlias)
        }.apply {
            sessionAlias = newSessionAlias
        }.also {
            assertEquals(MESSAGE_TYPE_VALUE, it.messageType)
            assertEquals(DIRECTION_VALUE, it.direction)
            assertEquals(newSessionAlias, it.sessionAlias)
        }
    }

    @Test
    fun `update sequence`() {
        val newSequence = Long.MAX_VALUE
        message(MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE).also {
            assertNotEquals(newSequence, it.sequence)
        }.apply {
            sequence = newSequence
        }.also {
            assertEquals(MESSAGE_TYPE_VALUE, it.messageType)
            assertEquals(DIRECTION_VALUE, it.direction)
            assertEquals(SESSION_ALIAS_VALUE, it.sessionAlias)
            assertEquals(newSequence, it.sequence)
        }
    }
}

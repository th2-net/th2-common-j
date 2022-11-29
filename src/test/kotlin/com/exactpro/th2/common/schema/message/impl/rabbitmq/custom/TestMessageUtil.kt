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

import com.exactpro.th2.common.event.bean.BaseTest.BOOK_NAME
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.bookName
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.fromJson
import com.exactpro.th2.common.message.get
import com.exactpro.th2.common.message.getList
import com.exactpro.th2.common.message.getMessage
import com.exactpro.th2.common.message.getString
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.set
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.updateList
import com.exactpro.th2.common.message.updateMessage
import com.exactpro.th2.common.message.updateOrAddString
import com.exactpro.th2.common.message.updateString
import com.exactpro.th2.common.value.updateString
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

private const val MESSAGE_TYPE_VALUE = "test message type"
private const val SESSION_ALIAS_VALUE = "test session alias"
private const val FIELD_NAME = "test field"
private const val FIELD_NAME_2 = "test field 2"
private const val FIELD_VALUE = "test value"
private const val FIELD_VALUE_2 = "test value 2"
private val DIRECTION_VALUE = SECOND

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
        message(BOOK_NAME, MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE).build().also {
            assertEquals(BOOK_NAME, it.bookName)
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
    fun `update book name`() {
        val builder = message(BOOK_NAME, MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE)
        val newBookName = builder.bookName + "Hello"

        builder.apply {
            bookName = newBookName
        }.also {
            assertEquals(newBookName, it.bookName)
            assertEquals(MESSAGE_TYPE_VALUE, it.messageType)
            assertEquals(DIRECTION_VALUE, it.direction)
            assertEquals(SESSION_ALIAS_VALUE, it.sessionAlias)
        }
    }
    
    @Test
    fun `update message type`() {
        val builder = message(BOOK_NAME, MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE)
        val newMessageType = builder.messageType + "Hello"

        builder.apply {
            messageType = newMessageType
        }.also {
            assertEquals(BOOK_NAME, it.bookName)
            assertEquals(newMessageType, it.messageType)
            assertEquals(DIRECTION_VALUE, it.direction)
            assertEquals(SESSION_ALIAS_VALUE, it.sessionAlias)
        }
    }

    @Test
    fun `update direction`() {
        val builder = message(BOOK_NAME, MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE)
        val newDirection = Direction.values().asSequence()
            .filter{ item -> item != Direction.UNRECOGNIZED && item != builder.direction }
            .first()

        builder.apply {
            direction = newDirection
        }.also {
            assertEquals(BOOK_NAME, it.bookName)
            assertEquals(MESSAGE_TYPE_VALUE, it.messageType)
            assertEquals(newDirection, it.direction)
            assertEquals(SESSION_ALIAS_VALUE, it.sessionAlias)
        }
    }

    @Test
    fun `update session alias`() {
        val builder = message(BOOK_NAME, MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE)
        val newSessionAlias = builder.sessionAlias + "Hello"

        builder.apply {
            sessionAlias = newSessionAlias
        }.also {
            assertEquals(BOOK_NAME, it.bookName)
            assertEquals(MESSAGE_TYPE_VALUE, it.messageType)
            assertEquals(DIRECTION_VALUE, it.direction)
            assertEquals(newSessionAlias, it.sessionAlias)
        }
    }

    @Test
    fun `update sequence`() {
        val builder = message(BOOK_NAME, MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE)
        val newSequence = builder.sequence++

        builder.apply {
            sequence = newSequence
        }.also {
            assertEquals(BOOK_NAME, it.bookName)
            assertEquals(MESSAGE_TYPE_VALUE, it.messageType)
            assertEquals(DIRECTION_VALUE, it.direction)
            assertEquals(SESSION_ALIAS_VALUE, it.sessionAlias)
            assertEquals(newSequence, it.sequence)
        }
    }

    @Test
    fun `from json message test`() {
        message().fromJson("""
            {
                "metadata": {
                    "id": {
                        "connectionId": {
                            "sessionAlias": "$SESSION_ALIAS_VALUE"
                        },
                        "bookName": "$BOOK_NAME"
                    },
                    "messageType": "$MESSAGE_TYPE_VALUE"
                },
                "fields": {
                    "$FIELD_NAME": {
                        "simpleValue": "$FIELD_VALUE"
                    }
                }
            }
        """.trimIndent()).also {
            assertEquals(BOOK_NAME, it.bookName)
            assertEquals(MESSAGE_TYPE_VALUE, it.messageType)
            assertEquals(SESSION_ALIAS_VALUE, it.sessionAlias)
            assertEquals(it.getString(FIELD_NAME), FIELD_VALUE)
        }
    }

    @Test
    fun `to json from json message test`() {
        val json = message(BOOK_NAME, MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE).apply {
            addField(FIELD_NAME, FIELD_VALUE)
        }.toJson()

        message().fromJson(json).also {
            assertEquals(BOOK_NAME, it.bookName)
            assertEquals(MESSAGE_TYPE_VALUE, it.messageType)
            assertEquals(DIRECTION_VALUE, it.direction)
            assertEquals(SESSION_ALIAS_VALUE, it.sessionAlias)
            assertEquals(FIELD_VALUE, it.getString(FIELD_NAME))
        }
    }

    @Test
    fun `update field`() {
        message(BOOK_NAME, MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE).apply {
            addField(FIELD_NAME, FIELD_VALUE)
        }.updateString(FIELD_NAME) { FIELD_VALUE_2 }.also {
            assertEquals(it.getString(FIELD_NAME), FIELD_VALUE_2)
        }
    }

    @Test
    fun `update complex field`() {
        message(BOOK_NAME, MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE).apply {
            addField(FIELD_NAME, message()
                .addField(FIELD_NAME, FIELD_VALUE)
                .addField(FIELD_NAME_2, listOf(FIELD_VALUE, FIELD_VALUE_2))
            )
        }.updateMessage(FIELD_NAME) {
            updateList(FIELD_NAME_2) {
                updateString(0) { FIELD_VALUE_2 }
            }
        }.also {
            assertEquals(it.getMessage(FIELD_NAME)?.getList(FIELD_NAME_2)?.map { it.simpleValue }, listOf(FIELD_VALUE_2, FIELD_VALUE_2))
        }
    }

    @Test
    fun `update or add field test add`() {
        message(BOOK_NAME, MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE).apply {
            updateOrAddString(FIELD_NAME) { it ?: FIELD_VALUE }
        }.also {
            assertEquals(it.getString(FIELD_NAME), FIELD_VALUE)
        }
    }

    @Test
    fun `update or add field test update`() {
        message(BOOK_NAME, MESSAGE_TYPE_VALUE, DIRECTION_VALUE, SESSION_ALIAS_VALUE).apply {
            addField(FIELD_NAME, FIELD_VALUE)
            updateOrAddString(FIELD_NAME) { it ?: FIELD_VALUE_2 }
        }.also {
            assertEquals(it.getString(FIELD_NAME), FIELD_VALUE)
        }
    }
}

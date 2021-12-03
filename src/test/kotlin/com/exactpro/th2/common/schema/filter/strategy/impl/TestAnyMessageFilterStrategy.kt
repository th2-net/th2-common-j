/*
 * Copyright 2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.filter.strategy.impl

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterOperation
import com.exactpro.th2.common.schema.message.configuration.MqRouterFilterConfiguration
import org.apache.commons.collections4.MultiMapUtils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource

class TestAnyMessageFilterStrategy {
    private val strategy = AnyMessageFilterStrategy()

    @ParameterizedTest
    @MethodSource("parsedMessages")
    fun `matches the parsed message by message type with single filter`(anyMessage: AnyMessage, expectMatch: Boolean) {
        val match = strategy.verify(
            anyMessage,
            MqRouterFilterConfiguration(
                metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put("message_type", FieldFilterConfiguration(
                        fieldName = "message_type",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "test"
                    ))
                }
            ))

        assertEquals(expectMatch, match) { "The message ${anyMessage.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messages")
    fun `matches the parsed message by direction with single filter`(message: AnyMessage, expectMatch: Boolean) {
        val match = strategy.verify(
            message,
            MqRouterFilterConfiguration(
                metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put("direction", FieldFilterConfiguration(
                        fieldName = "direction",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "FIRST"
                    ))
                }
            ))

        assertEquals(expectMatch, match) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messages")
    fun `matches the parsed message by alias with single filter`(message: AnyMessage, expectMatch: Boolean) {
        val match = strategy.verify(
            message,
            MqRouterFilterConfiguration(
                metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put("session_alias", FieldFilterConfiguration(
                        fieldName = "session_alias",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "test-alias"
                    ))
                }
            ))

        assertEquals(expectMatch, match) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    companion object {
        private val BOOK_NAME = BoxConfiguration().bookName

        private val PARSED_MESSAGE_MATCH = AnyMessage.newBuilder().setMessage(
            message(BOOK_NAME, "test", Direction.FIRST, "test-alias")
        ).build()

        private val RAW_MESSAGE_MATCH = AnyMessage.newBuilder().setRawMessage(
            RawMessage.newBuilder().apply {
                metadataBuilder.idBuilder.apply {
                    connectionIdBuilder.sessionAlias = "test-alias"
                    direction = Direction.FIRST
                }
            }
        ).build()

        private val PARSED_MESSAGE_MISS_MATCH = AnyMessage.newBuilder().setMessage(
            message(BOOK_NAME, "test1", Direction.SECOND, "test-alias1")
        ).build()

        private val RAW_MESSAGE_MISS_MATCH = AnyMessage.newBuilder().setRawMessage(
            RawMessage.newBuilder().apply {
                metadataBuilder.idBuilder.apply {
                    connectionIdBuilder.sessionAlias = "test-alias1"
                    direction = Direction.SECOND
                }
            }
        ).build()

        @JvmStatic
        fun parsedMessages(): List<Arguments> = listOf(
            arguments(PARSED_MESSAGE_MATCH, true),
            arguments(PARSED_MESSAGE_MISS_MATCH, false)
        )

        @JvmStatic
        fun messages(): List<Arguments> = listOf(
            arguments(RAW_MESSAGE_MATCH, true),
            arguments(RAW_MESSAGE_MISS_MATCH, false)
        ) + parsedMessages()
    }
}
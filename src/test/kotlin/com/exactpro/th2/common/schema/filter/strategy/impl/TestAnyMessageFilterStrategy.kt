/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.event.bean.BaseTest.BOOK_NAME
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.filter.strategy.impl.AnyMessageFilterStrategy.verify
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

    @ParameterizedTest
    @MethodSource("multipleFiltersMatch")
    fun `matches any filter`(anyMessage: AnyMessage, expectMatch: Boolean) {
        val match = verify(
            anyMessage,
            listOf(
                MqRouterFilterConfiguration(
                    metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                        put("message_type", FieldFilterConfiguration(
                            fieldName = "message_type",
                            operation = FieldFilterOperation.EQUAL,
                            expectedValue = "test"
                        ))
                    }
                ),
                MqRouterFilterConfiguration(
                    metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                        put("direction", FieldFilterConfiguration(
                            fieldName = "direction",
                            operation = FieldFilterOperation.EQUAL,
                            expectedValue = "FIRST"
                        ))
                    }
                )
            )
        )
        assertEquals(expectMatch, match) { "The message ${anyMessage.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("rawMessagesBothFilters")
    fun `matches with multiple metadata filters`(anyMessage: AnyMessage, expectMatch: Boolean) {
        val match = verify(
            anyMessage,
            MqRouterFilterConfiguration(
                metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put("session_alias", FieldFilterConfiguration(
                        fieldName = "session_alias",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "test-alias"
                    ))
                    put("direction", FieldFilterConfiguration(
                        fieldName = "direction",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "FIRST"
                    ))
                }
            )
        )
        assertEquals(expectMatch, match) { "The message ${anyMessage.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("parsedMessagesBothFilters")
    fun `matches with multiple message filters`(anyMessage: AnyMessage, expectMatch: Boolean) {
        val match = verify(
            anyMessage,
            MqRouterFilterConfiguration(
                message = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put("test-field1", FieldFilterConfiguration(
                        fieldName = "test-field1",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "test-value1"
                    ))
                    put("test-field2", FieldFilterConfiguration(
                        fieldName = "test-field2",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "test-value2"
                    ))
                }
            )
        )
        assertEquals(expectMatch, match) { "The message ${anyMessage.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messagesWithProperties")
    fun `matches with multiple properties filters`(anyMessage: AnyMessage, expectMatch: Boolean) {
        val match = verify(
            anyMessage,
            MqRouterFilterConfiguration(
                metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put("prop-field1", FieldFilterConfiguration(
                        fieldName = "prop-field1",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "prop-value1"
                    ))
                    put("prop-field2", FieldFilterConfiguration(
                        fieldName = "prop-field2",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "prop-value2"
                    ))
                }
            )
        )
        assertEquals(expectMatch, match) { "The message ${anyMessage.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messagesWithMessageAndMetadataFilters")
    fun `matches with multiple message and metadata filters`(anyMessage: AnyMessage, expectMatch: Boolean) {
        val match = verify(
            anyMessage,
            MqRouterFilterConfiguration(
                message = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put("test-field1", FieldFilterConfiguration(
                        fieldName = "test-field1",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "test-value1"
                    ))
                    put("test-field2", FieldFilterConfiguration(
                        fieldName = "test-field2",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "test-value2"
                    ))
                },
                metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put("message_type", FieldFilterConfiguration(
                        fieldName = "message_type",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "test"
                    ))
                    put("direction", FieldFilterConfiguration(
                        fieldName = "direction",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "FIRST"
                    ))
                }
            )
        )
        assertEquals(expectMatch, match) { "The message ${anyMessage.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messageWithProtocol")
    fun `matches protocol metadata filter`(anyMessage: AnyMessage, expectMatch: Boolean) {
        val match = verify(
            anyMessage,
            MqRouterFilterConfiguration(
                metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put("protocol", FieldFilterConfiguration(
                        fieldName = "protocol",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "HTTP"
                    ))
                }
            )
        )
        assertEquals(expectMatch, match) { "The message ${anyMessage.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("parsedMessages")
    fun `matches the parsed message by message type with single filter`(anyMessage: AnyMessage, expectMatch: Boolean) {
        val match = verify(
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
        val match = verify(
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
        val match = verify(
            message,
            MqRouterFilterConfiguration(
                metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put("session_alias", FieldFilterConfiguration(
                        fieldName = "session_alias",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "test-alias"
                    ))
                }
            )
        )

        assertEquals(expectMatch, match) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messagesWithSameFilterFields")
    fun `miss matches with the same filter fields`(message: AnyMessage, expectMatch: Boolean) {
        val match = verify(
            message,
            MqRouterFilterConfiguration(
                message = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put(
                        "test-field", FieldFilterConfiguration(
                            fieldName = "test-field",
                            operation = FieldFilterOperation.EQUAL,
                            expectedValue = "test-value1"
                        )
                    )
                    put(
                        "test-field", FieldFilterConfiguration(
                            fieldName = "test-field",
                            operation = FieldFilterOperation.EQUAL,
                            expectedValue = "test-value2"
                        )
                    )
                }
            )
        )

        assertEquals(expectMatch, match) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messagesWithMultipleFiltersWithSameFilterField")
    fun `matches with multiple filters with the same filter fields`(message: AnyMessage, expectMatch: Boolean) {
        val match = verify(
            message,
            listOf(
                MqRouterFilterConfiguration(
                    message = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                        put(
                            "test-field", FieldFilterConfiguration(
                                fieldName = "test-field",
                                operation = FieldFilterOperation.EQUAL,
                                expectedValue = "test-value1"
                            )
                        )
                    }
                ),
                MqRouterFilterConfiguration(
                    message = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                        put(
                            "test-field", FieldFilterConfiguration(
                                fieldName = "test-field",
                                operation = FieldFilterOperation.EQUAL,
                                expectedValue = "test-value2"
                            )
                        )
                    }
                )
            )
        )

        assertEquals(expectMatch, match) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messagesWithMultipleSameFields")
    fun `matches message with multiple fields with same name`(message: AnyMessage, expectMatch: Boolean) {
        val match = verify(
            message,
            MqRouterFilterConfiguration(
                message = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put(
                        "test-field", FieldFilterConfiguration(
                            fieldName = "test-field",
                            operation = FieldFilterOperation.NOT_EQUAL,
                            expectedValue = "test-value1"
                        )
                    )
                    put(
                        "test-field", FieldFilterConfiguration(
                            fieldName = "test-field",
                            operation = FieldFilterOperation.EQUAL,
                            expectedValue = "test-value2"
                        )
                    )
                }
            )
        )

        assertEquals(expectMatch, match) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messagesWithOneProperty")
    fun `matches message with properties`(message: AnyMessage, expectMatch: Boolean) {
        val match = verify(
            message,
            MqRouterFilterConfiguration(
                metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put(
                        "test-property", FieldFilterConfiguration(
                            fieldName = "test-property",
                            operation = FieldFilterOperation.EQUAL,
                            expectedValue = "property-value"
                        )
                    )
                }
            )
        )

        assertEquals(expectMatch, match) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messagesWithPropertiesAndMetadata")
    fun `matches message with properties and metadata`(message: AnyMessage, expectMatch: Boolean) {
        val match = verify(
            message,
            MqRouterFilterConfiguration(
                metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put(
                        "test-property", FieldFilterConfiguration(
                            fieldName = "test-property",
                            operation = FieldFilterOperation.EQUAL,
                            expectedValue = "property-value"
                        )
                    )

                    put("message_type", FieldFilterConfiguration(
                        fieldName = "message_type",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "test"
                    ))
                }
            )
        )

        assertEquals(expectMatch, match) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    companion object {
        private fun simpleMessageBuilder(messageType: String, direction: Direction, sessionAlias: String): AnyMessage {
            return AnyMessage.newBuilder().setMessage(
                message(BOOK_NAME, messageType, direction, sessionAlias)
            ).build()
        }

        private fun simpleRawMessageBuilder(sessionAlias: String, directionValue: Direction): AnyMessage {
            return AnyMessage.newBuilder().setRawMessage(
                RawMessage.newBuilder().apply {
                    metadataBuilder.idBuilder.apply {
                        connectionIdBuilder.sessionAlias = sessionAlias
                        direction = directionValue
                    }
                }
            ).build()
        }

        private fun messageWithFieldsBuilder(messageType: String, direction: Direction, fields: List<Pair<String, String>>): AnyMessage {
            return AnyMessage.newBuilder().setMessage(
                message(BOOK_NAME, messageType, direction, "test-alias").apply {
                    fields.forEach { addField(it.first, it.second) }
                }
            ).build()
        }

        private fun messageWithPropertiesBuilder(messageType: String, direction: Direction, properties: List<Pair<String, String>>): AnyMessage {
            return AnyMessage.newBuilder().setMessage(
                message(BOOK_NAME, messageType, direction, "test-alias").apply {
                    properties.forEach { metadataBuilder.putProperties(it.first, it.second) }
                }
            ).build()
        }

        private fun rawMessageWithOnePropertyBuilder(propertyKey: String, propertyValue: String): AnyMessage {
            return AnyMessage.newBuilder().setRawMessage(
                RawMessage.newBuilder().apply {
                    metadataBuilder.putProperties(propertyKey, propertyValue)
                }
            ).build()
        }

        private fun messageWithOnePropertyBuilder(messageType: String, propertyKey: String, propertyValue: String): AnyMessage {
            return messageWithPropertiesBuilder(messageType, Direction.FIRST, listOf(Pair(propertyKey, propertyValue)))
        }

        private fun messageWithProtocolBuilder(protocol: String): AnyMessage {
            return AnyMessage.newBuilder().setMessage(
                message(BOOK_NAME, "test", Direction.FIRST, "test-alias").apply {
                    metadataBuilder.protocol = protocol
                }
            ).build()
        }

        @JvmStatic
        fun messageWithProtocol(): List<Arguments> = listOf(
            arguments(messageWithProtocolBuilder("HTTP"), true),
            arguments(messageWithProtocolBuilder("FTP"), false),
        )

        @JvmStatic
        fun messages(): List<Arguments> = listOf(
            arguments(simpleRawMessageBuilder("test-alias", Direction.FIRST), true),
            arguments(simpleRawMessageBuilder("test-alias1", Direction.SECOND), false)
        ) + parsedMessages()

        @JvmStatic
        fun parsedMessages(): List<Arguments> = listOf(
            arguments(simpleMessageBuilder("test", Direction.FIRST, "test-alias"), true),
            arguments(simpleMessageBuilder("test1", Direction.SECOND, "test-alias1"), false)
        )

        @JvmStatic
        fun messagesWithProperties(): List<Arguments> = listOf(
            arguments(messageWithPropertiesBuilder("test", Direction.FIRST, listOf(
                Pair("prop-field1", "prop-value1"), Pair("prop-field2", "prop-value2"))), true),
            arguments(messageWithPropertiesBuilder("test", Direction.FIRST, listOf(
                Pair("prop-field1", "prop-value-wrong"), Pair("prop-field2", "prop-value2"))), false),
            arguments(messageWithPropertiesBuilder("test", Direction.FIRST, listOf(
                Pair("prop-field1", "prop-value1"), Pair("prop-field2", "prop-value-wrong"))), false),
            arguments(messageWithPropertiesBuilder("test", Direction.FIRST, listOf(
                Pair("prop-field1", "prop-value-wrong"), Pair("prop-field2", "prop-value-wrong"))), false)
        )

        @JvmStatic
        fun messagesWithMultipleSameFields(): List<Arguments> = listOf(
            arguments(messageWithFieldsBuilder("test", Direction.FIRST,
                listOf(Pair("test-field", "test-value1"), Pair("test-field", "test-value2"))), true)
        )

        @JvmStatic
        fun messagesWithSameFilterFields(): List<Arguments> = listOf(
            arguments(messageWithFieldsBuilder("test", Direction.FIRST, listOf(Pair("test-field", "test-value1"))), false),
            arguments(messageWithFieldsBuilder("test", Direction.FIRST, listOf(Pair("test-field", "test-value2"))), false)
        )

        @JvmStatic
        fun messagesWithMultipleFiltersWithSameFilterField(): List<Arguments> = listOf(
            arguments(messageWithFieldsBuilder("test", Direction.FIRST, listOf(Pair("test-field", "test-value1"))), true),
            arguments(messageWithFieldsBuilder("test", Direction.FIRST, listOf(Pair("test-field", "test-value2"))), true)
        )

        @JvmStatic
        fun multipleFiltersMatch(): List<Arguments> = listOf(
            arguments(simpleMessageBuilder("test", Direction.FIRST, "test-alias"), true),
            arguments(simpleMessageBuilder("test", Direction.SECOND, "test-alias"), true),
            arguments(simpleMessageBuilder("test-wrong", Direction.FIRST, "test-alias"), true),
            arguments(simpleMessageBuilder("test-wrong", Direction.SECOND, "test-alias"), false)
        )

        @JvmStatic
        fun messagesWithMessageAndMetadataFilters() : List<Arguments> = listOf(
            // fields full match
            arguments(messageWithFieldsBuilder("test", Direction.FIRST,
                listOf(Pair("test-field1", "test-value1"), Pair("test-field2", "test-value2"))), true),

            // metadata mismatch
            arguments(messageWithFieldsBuilder("test", Direction.SECOND,
                listOf(Pair("test-field1", "test-value1"), Pair("test-field2", "test-value2"))), false),
            arguments(messageWithFieldsBuilder("test-wrong", Direction.FIRST,
                listOf(Pair("test-field1", "test-value1"), Pair("test-field2", "test-value2"))), false),

            // fields mismatch
            arguments(messageWithFieldsBuilder("test", Direction.FIRST,
                listOf(Pair("test-field1", "test-value-wrong"), Pair("test-field2", "test-value2"))), false),
            arguments(messageWithFieldsBuilder("test", Direction.FIRST,
                listOf(Pair("test-field1", "test-value1"), Pair("test-field2", "test-value-wrong"))), false),
            arguments(messageWithFieldsBuilder("test", Direction.FIRST,
                listOf(Pair("test-field1", "test-value-wrong"), Pair("test-field2", "test-value-wrong"))), false),

            // one field and one metadata mismatch
            arguments(messageWithFieldsBuilder("test", Direction.SECOND,
                listOf(Pair("test-field1", "test-value-wrong"), Pair("test-field2", "test-value2"))), false),
            arguments(messageWithFieldsBuilder("test-wrong", Direction.FIRST,
                listOf(Pair("test-field1", "test-value1"), Pair("test-field2", "test-value-wrong"))), false)
        )

        @JvmStatic
        fun parsedMessagesBothFilters() : List<Arguments> = listOf(
            arguments(messageWithFieldsBuilder("test", Direction.FIRST,
                listOf(Pair("test-field1", "test-value1"), Pair("test-field2", "test-value2"))), true),
            arguments(messageWithFieldsBuilder("test", Direction.FIRST,
                listOf(Pair("test-field1", "test-value-wrong"), Pair("test-field2", "test-value2"))), false),
            arguments(messageWithFieldsBuilder("test", Direction.FIRST,
                listOf(Pair("test-field1", "test-value1"), Pair("test-field2", "test-value-wrong"))), false),
            arguments(messageWithFieldsBuilder("test", Direction.FIRST,
                listOf(Pair("test-field1", "test-value-wrong"), Pair("test-field2", "test-value-wrong"))), false)
        )

        @JvmStatic
        fun messagesWithOneProperty() : List<Arguments> = listOf(
            arguments(messageWithOnePropertyBuilder("test", "test-property", "property-value"), true),
            arguments(messageWithOnePropertyBuilder("test", "test-property", "property-value-wrong"), false),
            arguments(rawMessageWithOnePropertyBuilder("test-property", "property-value"), true),
            arguments(rawMessageWithOnePropertyBuilder("test-property", "property-value-wrong"), false)
        )

        @JvmStatic
        fun messagesWithPropertiesAndMetadata() : List<Arguments> = listOf(
            arguments(messageWithOnePropertyBuilder("test", "test-property", "property-value"), true),
            arguments(messageWithOnePropertyBuilder("test", "test-property", "property-value-wrong"), false),
            arguments(messageWithOnePropertyBuilder("test-wrong", "test-property", "property-value"), false)
        )

        @JvmStatic
        fun rawMessagesBothFilters() : List<Arguments> = listOf(
            arguments(simpleRawMessageBuilder("test-alias", Direction.FIRST), true),
            arguments(simpleRawMessageBuilder("test-alias", Direction.SECOND), false),
            arguments(simpleRawMessageBuilder("test-alias-wrong-value", Direction.FIRST), false),
            arguments(simpleRawMessageBuilder("test-alias-wrong-value", Direction.SECOND), false)
        )
    }
}
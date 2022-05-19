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
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.toJson
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
    @MethodSource("multipleFiltersMatch")
    fun `matches any filter`(anyMessage: AnyMessage, expectMatch: Boolean) {
        val match = strategy.verify(
            anyMessage,
            listOf(
                MqRouterFilterConfiguration(
                    metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                        put("session_alias", FieldFilterConfiguration(
                            fieldName = "session_alias",
                            operation = FieldFilterOperation.EQUAL,
                            expectedValue = "test-alias"
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
                ),
            )
        )
        assertEquals(expectMatch, match) { "The message ${anyMessage.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("rawMessagesBothFilters")
    fun `matches with multiple metadata filters`(anyMessage: AnyMessage, expectMatch: Boolean) {
        val match = strategy.verify(
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
        val match = strategy.verify(
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
    @MethodSource("messagesWithMessageAndMetadataFilters")
    fun `matches with multiple message and metadata filters`(anyMessage: AnyMessage, expectMatch: Boolean) {
        val match = strategy.verify(
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

    @ParameterizedTest
    @MethodSource("messagesWithSameFilterFields")
    fun `miss matches with the same filter fields`(message: AnyMessage, expectMatch: Boolean) {
        val match = strategy.verify(
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
                },
            )
        )

        assertEquals(
            expectMatch,
            match
        ) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messagesWithMultipleFiltersWithSameFilterField")
    fun `matches with multiple filters with the same filter fields`(message: AnyMessage, expectMatch: Boolean) {
        val match = strategy.verify(
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
                    },
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
                    },
                ),

            )
        )

        assertEquals(
            expectMatch,
            match
        ) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messagesWithMultipleSameFields")
    fun `matches message with multiple fields with same name`(message: AnyMessage, expectMatch: Boolean) {
        val match = strategy.verify(
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
                },
            )
        )

        assertEquals(
            expectMatch,
            match
        ) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messagesWithProperties")
    fun `matches message with properties`(message: AnyMessage, expectMatch: Boolean) {
        val match = strategy.verify(
            message,
            MqRouterFilterConfiguration(
                properties = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put(
                        "test-property", FieldFilterConfiguration(
                            fieldName = "test-property",
                            operation = FieldFilterOperation.EQUAL,
                            expectedValue = "property-value"
                        )
                    )
                },
            )
        )

        assertEquals(
            expectMatch,
            match
        ) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    @ParameterizedTest
    @MethodSource("messagesWithPropertiesAndMetadata")
    fun `matches message with properties and metadata`(message: AnyMessage, expectMatch: Boolean) {
        val match = strategy.verify(
            message,
            MqRouterFilterConfiguration(
                properties = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put(
                        "test-property", FieldFilterConfiguration(
                            fieldName = "test-property",
                            operation = FieldFilterOperation.EQUAL,
                            expectedValue = "property-value"
                        )
                    )
                },
                metadata = MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().apply {
                    put("session_alias", FieldFilterConfiguration(
                        fieldName = "session_alias",
                        operation = FieldFilterOperation.EQUAL,
                        expectedValue = "test-alias"
                    ))
                }
            )
        )

        assertEquals(
            expectMatch,
            match
        ) { "The message ${message.toJson()} was${if (expectMatch) "" else " not"} matched" }
    }

    companion object {

        private fun simpleMessageBuilder(messageType: String, direction: Direction, sessionAlias: String): AnyMessage {
            return AnyMessage.newBuilder().setMessage(
                message(messageType, direction, sessionAlias)
            ).build()
        }

        private val PARSED_MESSAGE_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias")
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
            message("test1", Direction.SECOND, "test-alias1")
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

        /////////////////

        private val MATCHES_WITH_SAME_FIELDS = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field", "test-value1")
                addField("test-field", "test-value2")
            }
        ).build()

        @JvmStatic
        fun messagesWithMultipleSameFields(): List<Arguments> = listOf(
            arguments(MATCHES_WITH_SAME_FIELDS, true),
        )

        ////////////////

        private val MESSAGE_WITH_ONE_FIELD_1 = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field", "test-value1")
            }
        ).build()
        private val MESSAGE_WITH_ONE_FIELD_2 = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field", "test-value2")
            }
        ).build()

        @JvmStatic
        fun messagesWithSameFilterFields(): List<Arguments> = listOf(
            arguments(MESSAGE_WITH_ONE_FIELD_1, false),
            arguments(MESSAGE_WITH_ONE_FIELD_2, false),
        )

        @JvmStatic
        fun messagesWithMultipleFiltersWithSameFilterField(): List<Arguments> = listOf(
            arguments(MESSAGE_WITH_ONE_FIELD_1, true),
            arguments(MESSAGE_WITH_ONE_FIELD_2, true),
        )

        ///////////////

        @JvmStatic
        fun multipleFiltersMatch(): List<Arguments> = listOf(
            arguments(simpleMessageBuilder("test", Direction.FIRST, "test-alias"), true),
            arguments(simpleMessageBuilder("test", Direction.SECOND, "test-alias"), true),
            arguments(simpleMessageBuilder("test", Direction.FIRST, "test-wrong"), true),
            arguments(simpleMessageBuilder("test", Direction.SECOND, "test-alias-wrong"), false),
        )

        /////////////////

        private val MESSAGE_WITH_FIELDS_FULL_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field1", "test-value1")
                addField("test-field2", "test-value2")
            }
        ).build()

        private val MESSAGE_WITH_FIELDS_METADATA_MISMATCH_1 = AnyMessage.newBuilder().setMessage(
            message("test", Direction.SECOND, "test-alias").apply {
                addField("test-field1", "test-value1")
                addField("test-field2", "test-value2")
            }
        ).build()
        private val MESSAGE_WITH_FIELDS_METADATA_MISMATCH_2 = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias-wrong").apply {
                addField("test-field1", "test-value1")
                addField("test-field2", "test-value2")
            }
        ).build()

        private val MESSAGE_WITH_FIELDS_FIELD_MISMATCH_1 = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field1", "test-value-wrong")
                addField("test-field2", "test-value2")
            }
        ).build()
        private val MESSAGE_WITH_FIELDS_FIELD_MISMATCH_2 = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field1", "test-value1")
                addField("test-field2", "test-value-wrong")
            }
        ).build()

        private val MESSAGE_WITH_FIELDS_FIELD_AND_METADATA_MISMATCH_1 = AnyMessage.newBuilder().setMessage(
            message("test", Direction.SECOND, "test-alias").apply {
                addField("test-field1", "test-value-wrong")
                addField("test-field2", "test-value2")
            }
        ).build()
        private val MESSAGE_WITH_FIELDS_FIELD_AND_METADATA_MISMATCH_2 = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias-wrong").apply {
                addField("test-field1", "test-value1")
                addField("test-field2", "test-value-wrong")
            }
        ).build()

        private val MESSAGE_WITH_FIELDS_ALL_FIELDS_MISMATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field1", "test-value-wrong")
                addField("test-field2", "test-value-wrong")
            }
        ).build()

        @JvmStatic
        fun messagesWithMessageAndMetadataFilters() : List<Arguments> = listOf(
            arguments(MESSAGE_WITH_FIELDS_FULL_MATCH, true),
            arguments(MESSAGE_WITH_FIELDS_METADATA_MISMATCH_1, false),
            arguments(MESSAGE_WITH_FIELDS_METADATA_MISMATCH_2, false),
            arguments(MESSAGE_WITH_FIELDS_FIELD_MISMATCH_1, false),
            arguments(MESSAGE_WITH_FIELDS_FIELD_MISMATCH_2, false),
            arguments(MESSAGE_WITH_FIELDS_FIELD_AND_METADATA_MISMATCH_1, false),
            arguments(MESSAGE_WITH_FIELDS_FIELD_AND_METADATA_MISMATCH_2, false),
            arguments(MESSAGE_WITH_FIELDS_ALL_FIELDS_MISMATCH, false),
        )

        @JvmStatic
        fun parsedMessagesBothFilters() : List<Arguments> = listOf(
            arguments(MESSAGE_WITH_FIELDS_FULL_MATCH, true),
            arguments(MESSAGE_WITH_FIELDS_FIELD_MISMATCH_1, false),
            arguments(MESSAGE_WITH_FIELDS_FIELD_MISMATCH_2, false),
            arguments(MESSAGE_WITH_FIELDS_ALL_FIELDS_MISMATCH, false),
        )

        /////////////

        private val RAW_MESSAGE_WITH_PROPERTY = AnyMessage.newBuilder().setRawMessage(
            RawMessage.newBuilder().apply {
                metadataBuilder.putProperties("test-property", "property-value")
            }
        ).build()

        private val RAW_MESSAGE_WITH_PROPERTY_MISMATCH = AnyMessage.newBuilder().setRawMessage(
            RawMessage.newBuilder().apply {
                metadataBuilder.putProperties("test-property", "property-value-wrong")
            }
        ).build()

        private val MESSAGE_WITH_PROPERTIES = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                metadataBuilder.putProperties("test-property", "property-value")
            }
        ).build()

        private val MESSAGE_WITH_PROPERTIES_PROPERTY_MISMATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                metadataBuilder.putProperties("test-property", "property-value-wrong")
            }
        ).build()

        private val MESSAGE_WITH_PROPERTIES_METADATA_MISMATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias-wrong").apply {
                metadataBuilder.putProperties("test-property", "property-value")
            }
        ).build()

        @JvmStatic
        fun messagesWithProperties() : List<Arguments> = listOf(
            arguments(MESSAGE_WITH_PROPERTIES, true),
            arguments(MESSAGE_WITH_PROPERTIES_PROPERTY_MISMATCH, false),
            arguments(RAW_MESSAGE_WITH_PROPERTY, true),
            arguments(RAW_MESSAGE_WITH_PROPERTY_MISMATCH, false),
        )

        @JvmStatic
        fun messagesWithPropertiesAndMetadata() : List<Arguments> = listOf(
            arguments(MESSAGE_WITH_PROPERTIES, true),
            arguments(MESSAGE_WITH_PROPERTIES_PROPERTY_MISMATCH, false),
            arguments(MESSAGE_WITH_PROPERTIES_METADATA_MISMATCH, false),
        )

        /////////////

        private val RAW_MESSAGE_FULL_MATCH = AnyMessage.newBuilder().setRawMessage(
            RawMessage.newBuilder().apply {
                metadataBuilder.idBuilder.apply {
                    connectionIdBuilder.sessionAlias = "test-alias"
                    direction = Direction.FIRST
                }
            }
        ).build()

        private val RAW_MESSAGE_ONE_MISMATCH = AnyMessage.newBuilder().setRawMessage(
            RawMessage.newBuilder().apply {
                metadataBuilder.idBuilder.apply {
                    connectionIdBuilder.sessionAlias = "test-alias"
                    direction = Direction.SECOND
                }
            }
        ).build()

        private val RAW_MESSAGE_NO_MATCH = AnyMessage.newBuilder().setRawMessage(
            RawMessage.newBuilder().apply {
                metadataBuilder.idBuilder.apply {
                    connectionIdBuilder.sessionAlias = "test-alias-wrong-value"
                    direction = Direction.SECOND
                }
            }
        ).build()

        @JvmStatic
        fun rawMessagesBothFilters() : List<Arguments> = listOf(
            arguments(RAW_MESSAGE_FULL_MATCH, true),
            arguments(RAW_MESSAGE_ONE_MISMATCH, false),
            arguments(RAW_MESSAGE_NO_MATCH, false)
        )

    }
}
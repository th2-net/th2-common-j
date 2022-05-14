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

    companion object {
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

        private val MESSAGE_WITH_ONE_FIELDS_FOR_FILTER_WITH_SAME_FIELDS = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field", "test-value1")
            }
        ).build()

        @JvmStatic
        fun messagesWithSameFilterFields(): List<Arguments> = listOf(
            arguments(MESSAGE_WITH_ONE_FIELDS_FOR_FILTER_WITH_SAME_FIELDS, false),
        )

        @JvmStatic
        fun messagesWithMultipleFiltersWithSameFilterField(): List<Arguments> = listOf(
            arguments(MESSAGE_WITH_ONE_FIELDS_FOR_FILTER_WITH_SAME_FIELDS, true),
        )

        ///////////////

        private val MULTIPLE_FILTERS_MESSAGE_FULL_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias")
        ).build()

        private val MULTIPLE_FILTERS_MESSAGE_ONE_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.SECOND, "test-alias")
        ).build()

        private val MULTIPLE_FILTERS_MESSAGE_NOT_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.SECOND, "test-alias-wrong")
        ).build()

        @JvmStatic
        fun multipleFiltersMatch(): List<Arguments> = listOf(
            arguments(MULTIPLE_FILTERS_MESSAGE_FULL_MATCH, true),
            arguments(MULTIPLE_FILTERS_MESSAGE_ONE_MATCH, true),
            arguments(MULTIPLE_FILTERS_MESSAGE_NOT_MATCH, false),
        )

        /////////////////

        private val PARSED_MESSAGE_BOTH_FILTERS_MESSAGES_AND_METADATA_FULL_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field1", "test-value1")
                addField("test-field2", "test-value2")
            }
        ).build()

        private val PARSED_MESSAGE_BOTH_FILTERS_ONE_METADATA_NOT_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.SECOND, "test-alias").apply {
                addField("test-field1", "test-value1")
                addField("test-field2", "test-value2")
            }
        ).build()

        private val PARSED_MESSAGE_BOTH_FILTERS_ONE_MESSAGE_NOT_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field1", "test-value-wrong")
                addField("test-field2", "test-value2")
            }
        ).build()

        private val PARSED_MESSAGE_BOTH_FILTERS_ONE_MESSAGE_AND_ONE_METADATA_NOT_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.SECOND, "test-alias").apply {
                addField("test-field1", "test-value-wrong")
                addField("test-field2", "test-value2")
            }
        ).build()

        @JvmStatic
        fun messagesWithMessageAndMetadataFilters() : List<Arguments> = listOf(
            arguments(PARSED_MESSAGE_BOTH_FILTERS_MESSAGES_AND_METADATA_FULL_MATCH, true),
            arguments(PARSED_MESSAGE_BOTH_FILTERS_ONE_METADATA_NOT_MATCH, false),
            arguments(PARSED_MESSAGE_BOTH_FILTERS_ONE_MESSAGE_NOT_MATCH, false),
            arguments(PARSED_MESSAGE_BOTH_FILTERS_ONE_MESSAGE_AND_ONE_METADATA_NOT_MATCH, false)
        )

        ////////////

        private val PARSED_MESSAGE_BOTH_FILTERS_FULL_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field1", "test-value1")
                addField("test-field2", "test-value2")
            }
        ).build()

        private val PARSED_MESSAGE_BOTH_FILTERS_ONE_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field1", "test-value-wrong")
                addField("test-field2", "test-value2")
            }
        ).build()

        private val PARSED_MESSAGE_BOTH_FILTERS_NO_MATCH = AnyMessage.newBuilder().setMessage(
            message("test", Direction.FIRST, "test-alias").apply {
                addField("test-field1", "test-value-wrong")
                addField("test-field2", "test-value-wrong")
            }
        ).build()

        @JvmStatic
        fun parsedMessagesBothFilters() : List<Arguments> = listOf(
            arguments(PARSED_MESSAGE_BOTH_FILTERS_FULL_MATCH, true),
            arguments(PARSED_MESSAGE_BOTH_FILTERS_ONE_MATCH, false),
            arguments(PARSED_MESSAGE_BOTH_FILTERS_NO_MATCH, false),
        )

        /////////////

        private val RAW_MESSAGE_BOTH_FILTERS_FULL_MATCH = AnyMessage.newBuilder().setRawMessage(
            RawMessage.newBuilder().apply {
                metadataBuilder.idBuilder.apply {
                    connectionIdBuilder.sessionAlias = "test-alias"
                    direction = Direction.FIRST
                }
            }
        ).build()

        private val RAW_MESSAGE_BOTH_FILTERS_ONE_MATCH = AnyMessage.newBuilder().setRawMessage(
            RawMessage.newBuilder().apply {
                metadataBuilder.idBuilder.apply {
                    connectionIdBuilder.sessionAlias = "test-alias"
                    direction = Direction.SECOND
                }
            }
        ).build()

        private val RAW_MESSAGE_BOTH_FILTERS_NO_MATCH = AnyMessage.newBuilder().setRawMessage(
            RawMessage.newBuilder().apply {
                metadataBuilder.idBuilder.apply {
                    connectionIdBuilder.sessionAlias = "test-alias-wrong-value"
                    direction = Direction.SECOND
                }
            }
        ).build()

        @JvmStatic
        fun rawMessagesBothFilters() : List<Arguments> = listOf(
            arguments(RAW_MESSAGE_BOTH_FILTERS_FULL_MATCH, true),
            arguments(RAW_MESSAGE_BOTH_FILTERS_ONE_MATCH, false),
            arguments(RAW_MESSAGE_BOTH_FILTERS_NO_MATCH, false)
        )

    }
}
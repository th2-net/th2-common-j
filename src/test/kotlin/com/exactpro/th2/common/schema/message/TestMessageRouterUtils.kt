/*
 * Copyright 2021-2026 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.schema.message

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.google.protobuf.MessageOrBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class TestMessageRouterUtils {

    @ParameterizedTest
    @MethodSource("messageOrBuilderToJsonDataProvider")
    fun testMessageOrBuilderToJson(expected: String, data: MessageOrBuilder) {
        assertEquals(expected, data.toJson())
    }

    companion object {
        @JvmStatic
        fun messageOrBuilderToJsonDataProvider(): Stream<Arguments> {
            val expectedRaw = "{\"body\":\"\"}"
            val expectedMsg = "{\"fields\":{}}"

            return Stream.of(
                Arguments.of(expectedRaw, RawMessage.newBuilder()),
                Arguments.of(expectedRaw, RawMessage.newBuilder().build()),
                Arguments.of(expectedMsg, Message.newBuilder()),
                Arguments.of(expectedMsg, Message.newBuilder().build())
            )
        }
    }
}
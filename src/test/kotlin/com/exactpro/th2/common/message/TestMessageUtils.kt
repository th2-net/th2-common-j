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
package com.exactpro.th2.common.message

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.value.toListValue
import com.exactpro.th2.common.value.toValue
import com.google.protobuf.TextFormat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestMessageUtils {

    private val fieldText = """
        |message_filter { 
            |fields { key: "KeyEmpty" value { simple_filter: "" } } 
            |fields { key: "KeyString" value { simple_filter: "key string" } } 
            |fields { key: "NotKeyString" value { simple_filter: "not key field" } } 
            |fields { key: "SimpleCollection" value { list_filter { 
                |values { simple_filter: "a" } 
                |values { simple_filter: "b" } } } } }
    """.trimMargin().replace("\n", "")

    @Test
    fun `message to root message filter`() {
        createMessage().toRootMessageFilter(
            listOf("KeyString", "Message", "SimpleCollection"),
            listOf("prB"),
            listOf("KeyEmpty")
        ).also { messageFilter ->
            Assertions.assertNotNull(messageFilter)
            Assertions.assertEquals("""
                |messageType: "MsgType" 
                |message_filter { fields { key: "KeyEmpty" value { simple_filter: "" } } 
                    |fields { key: "KeyString" value { key: true simple_filter: "key string" } } 
                    |fields { key: "Message" value { key: true $fieldText } } 
                    |fields { key: "MessageCollection" value { list_filter { 
                        |values { $fieldText } 
                        |values { $fieldText } } } } 
                    |fields { key: "MessageTree" value { message_filter { 
                        |fields { key: "subMessageA" value { message_filter { 
                            |fields { key: "subMessageB" value { $fieldText } } } } } } } } 
                    |fields { key: "NotKeyString" value { simple_filter: "not key field" } } 
                    |fields { key: "SimpleCollection" value { key: true list_filter { 
                        |values { simple_filter: "a" } 
                        |values { simple_filter: "b" } } } } } 
                |metadata_filter { 
                    |property_filters { key: "prA" value { value: "A" } } 
                    |property_filters { key: "prB" value { key: true value: "B" } } } 
                |comparison_settings { ignore_fields: "KeyEmpty" }
            """.trimMargin().replace("\n", ""), TextFormat.shortDebugString(messageFilter))
        }
    }

    @Test
    fun `message without properties to root message filter`() {
        createMessage(addProperties = false).toRootMessageFilter(
            listOf("KeyString", "Message", "SimpleCollection"),
            listOf("prB"),
            listOf("KeyEmpty")
        ).also { messageFilter ->
            Assertions.assertNotNull(messageFilter)
            Assertions.assertEquals("""
                |messageType: "MsgType" 
                |message_filter { fields { key: "KeyEmpty" value { simple_filter: "" } } 
                    |fields { key: "KeyString" value { key: true simple_filter: "key string" } } 
                    |fields { key: "Message" value { key: true $fieldText } } 
                    |fields { key: "MessageCollection" value { list_filter { 
                        |values { $fieldText } 
                        |values { $fieldText } } } } 
                    |fields { key: "MessageTree" value { message_filter { 
                        |fields { key: "subMessageA" value { message_filter { 
                            |fields { key: "subMessageB" value { $fieldText } } } } } } } } 
                    |fields { key: "NotKeyString" value { simple_filter: "not key field" } } 
                    |fields { key: "SimpleCollection" value { key: true list_filter { 
                        |values { simple_filter: "a" } 
                        |values { simple_filter: "b" } } } } } 
                |comparison_settings { ignore_fields: "KeyEmpty" }
            """.trimMargin().replace("\n", ""), TextFormat.shortDebugString(messageFilter))
        }
    }

    private fun createMessage(addProperties: Boolean = true): Message = Message.newBuilder().apply {
        metadataBuilder.apply {
            messageType = "MsgType"
            if (addProperties) {
                putProperties("prA", "A")
                putProperties("prB", "B")
            }
        }

        fillMessage(this)
        putFields("Message", message { fillMessage(this) })
        putFields("MessageCollection", listOf(
            message { fillMessage(this) },
            message { fillMessage(this) }
        ).toListValue().toValue())
        putFields("MessageTree", message {
            putFields("subMessageA", message {
                putFields("subMessageB", message { fillMessage(this) })
            })
        })
    }.build()

    private fun fillMessage(builder: Message.Builder) {
        builder.putFields("KeyString", "key string".toValue())
            .putFields("NotKeyString", "not key field".toValue())
            .putFields("KeyEmpty", "".toValue())
            .putFields("SimpleCollection", listOf("a", "b").toListValue().toValue())
    }

    private fun message(block: Message.Builder.() -> Unit) = Value.newBuilder().apply {
        messageValueBuilder.block()
    }.build()
}
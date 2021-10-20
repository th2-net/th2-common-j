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

package com.exactpro.th2.common.message.tmp

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.tmp.Direction
import com.exactpro.th2.common.tmp.MessageBodyBuilder
import com.exactpro.th2.common.tmp.MessageFactory
import com.exactpro.th2.common.tmp.ParsedMessageBuilder
import com.exactpro.th2.common.tmp.ParsedMetadataBuilder
import com.exactpro.th2.common.tmp.RawMessageBuilder
import com.exactpro.th2.common.tmp.RawMetadataBuilder
import com.exactpro.th2.common.tmp.impl.ParsedMessageBuilderImpl
import com.exactpro.th2.common.tmp.impl.RawMessageBuilderImpl
import java.time.Instant
import java.util.function.Consumer

@DslMarker
annotation class BuilderExtension

operator fun ParsedMessageBuilderImpl.invoke(block: ParsedMessageBuilderImpl.() -> Unit): Message {
    return this.also(block).build()
}

operator fun RawMessageBuilderImpl.invoke(block: RawMessageBuilderImpl.() -> Unit): RawMessage {
    return this.also(block).build()
}

@BuilderExtension
class BodyBuilder(
    private val builder: MessageBodyBuilder
) {
    infix fun String.to(value: Any) {
        builder.putSimpleField(this, value)
    }

    infix fun String.to(values: Collection<Any>) {
        builder.putSimpleField(this, values)
    }

    infix fun String.toMessage(msgSetup: BodyBuilder.() -> Unit) {
        builder.putMessage(this) { BodyBuilder(it).msgSetup() }
    }

    infix fun String.toMessages(msgs: Collection<BodyBuilder.() -> Unit>) {
        builder.putMessages(this, msgs.map { action -> Consumer<MessageBodyBuilder> { BodyBuilder(it).action() } })
    }

    operator fun String.plusAssign(msgSetup: BodyBuilder.() -> Unit) {
        builder.addMessage(this) {
            BodyBuilder(it).msgSetup()
        }
    }

    operator fun invoke(block: BodyBuilder.() -> Unit) = block()
}

val ParsedMessageBuilder<*>.metadata: ParsedMetadataBuilder
    get() = metadataBuilder()

operator fun ParsedMetadataBuilder.invoke(block: ParsedMetadataBuilder.() -> Unit) = block()

val ParsedMessageBuilderImpl.body: BodyBuilder
    get() = BodyBuilder(this)

val RawMessageBuilder<*>.metadata: RawMetadataBuilder
    get() = metadataBuilder()

operator fun RawMetadataBuilder.invoke(block: RawMetadataBuilder.() -> Unit) = block()

fun MessageFactory.createParsedMessage(block: ParsedMessageBuilderImpl.() -> Unit): Message = createParsedMessage()(block)

fun MessageFactory.createRawMessage(block: RawMessageBuilderImpl.() -> Unit): RawMessage = createRawMessage()(block)

fun main() {
    val factory = MessageFactory()
    testParsedMessage(factory)
    testRawMessage(factory)
}

fun testParsedMessage(factory: MessageFactory) {
    val message = factory.createParsedMessage {
        setParentEventId("eventId")
        metadata {
            setSessionAlias("test")
            setDirection(Direction.SECOND)
            setSequence(1)
            addSubsequence(2)
            addSubsequence(3)
            setTimestamp(Instant.now())
            setMessageType("type")
            putProperty("propertyKey", "propertyValue")
            setProtocol("protocol")
        }
        body {
            "A" to 5
            "B" to listOf(1, 2, 3)
            "C" toMessage {
                "A" to 5
                "B" to listOf(1, 2, 3)
            }
            "D" += {
                "A" to 42
            }
            "E" toMessages listOf<BodyBuilder.() -> Unit>(
                {
                    "A" to 4
                },
                {
                    "A" to 5
                }
            )
        }
    }
    println(message.toJson(false))
}

fun testRawMessage(factory: MessageFactory) {
    val message = factory.createRawMessage {
        setParentEventId("eventId")
        setBody("body".toByteArray())
        metadata {
            setSessionAlias("test")
            setDirection(Direction.SECOND)
            setSequence(1)
            addSubsequence(2)
            addSubsequence(3)
            setTimestamp(Instant.now())
            putProperty("propertyKey", "propertyValue")
            setProtocol("protocol")
        }
    }
    println(message.toJson(false))
}
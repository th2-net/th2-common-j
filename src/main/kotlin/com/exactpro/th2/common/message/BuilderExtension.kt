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

package com.exactpro.th2.common.message

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.impl.ParsedMessageBuilderImpl
import com.exactpro.th2.common.message.impl.RawMessageBuilderImpl
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

    infix fun String.toMessages(msgSetups: Collection<BodyBuilder.() -> Unit>) {
        builder.putMessages(this, toConsumers(msgSetups))
    }

    operator fun String.plusAssign(msgSetup: BodyBuilder.() -> Unit) {
        builder.addMessage(this) {
            BodyBuilder(it).msgSetup()
        }
    }

    operator fun String.plusAssign(msgSetups: Collection<BodyBuilder.() -> Unit>) {
        builder.addMessages(this, toConsumers(msgSetups))
    }

    private fun toConsumers(msgSetups: Collection<BodyBuilder.() -> Unit>): List<Consumer<MessageBodyBuilder>> =
        msgSetups.map { action -> Consumer { BodyBuilder(it).action() } }

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
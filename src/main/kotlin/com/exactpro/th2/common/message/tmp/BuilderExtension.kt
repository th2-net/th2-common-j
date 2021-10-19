package com.exactpro.th2.common.message.tmp

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.tmp.MessageBodyBuilder
import com.exactpro.th2.common.tmp.MessageBuilder
import com.exactpro.th2.common.tmp.MessageFactory
import com.exactpro.th2.common.tmp.MetadataBuilder
import com.exactpro.th2.common.tmp.impl.ParsedMessageBuilder
import java.util.function.Consumer

@DslMarker
annotation class BuilderExtension

operator fun ParsedMessageBuilder.invoke(block: ParsedMessageBuilder.() -> Unit): Message {
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
//        builder.putMessages(this, msgs.map { action -> Consumer { BodyBuilder(it).action() } })
    }

    operator fun String.plusAssign(msgSetup: BodyBuilder.() -> Unit) {
        builder.addMessage(this) {
            BodyBuilder(it).msgSetup()
        }
    }

    operator fun invoke(block: BodyBuilder.() -> Unit) = block()
}

val MessageBuilder<*>.metadata: MetadataBuilder
    get() = metadataBuilder()

operator fun MetadataBuilder.invoke(block: MetadataBuilder.() -> Unit) = block()

val ParsedMessageBuilder.body: BodyBuilder
    get() = BodyBuilder(this)

fun MessageFactory.createParsedMessage(block: ParsedMessageBuilder.() -> Unit): Message = createParsedMessage().invoke(block)

fun main() {
    val factory = MessageFactory()
    val message = factory.createParsedMessage {
        metadata {
            setDirection(Direction.SECOND)
            setSessionAlias("test")
            setSequence(1)
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
//            "E" toMessages listOf(
//                {
//                    "A" to 4
//                },
//                {
//                    "A" to 5
//                }
//            )
        }
    }
    println(message.toJson(false))
}
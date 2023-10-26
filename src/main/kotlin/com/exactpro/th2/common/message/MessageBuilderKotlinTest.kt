package com.exactpro.th2.common.message

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.IEventFactory
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.factory.CommonFactory
import java.time.Instant

class MessageBuilderKotlinTest {

    companion object {

        @JvmStatic
        fun main(args: Array<String>) {
            val commonFactory = CommonFactory.createFromArguments("-c", "src/test/resources/test_load_dictionaries")
            val messageFactory = commonFactory.messageFactory
            val bookName = commonFactory.boxConfiguration.bookName!!
            testParsedMessage(messageFactory, bookName)
            testRawMessage(messageFactory, bookName)
            testEvent(commonFactory.eventFactory, bookName)
        }

        private fun testParsedMessage(factory: MessageFactory, bookName: String) {
            val message = factory.createParsedMessage {
                setParentEventId("eventId", bookName)
                metadata {
                    setSessionAlias("test")
                    setDirection(Direction.SECOND)
                    setSequence(1)
                    addSubsequence(2)
                    addSubsequence(3)
                    setBookName(bookName)
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
                    "D" += listOf(
                        { "A" to 43 },
                        { "A" to 44 }
                    )
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

        private fun testRawMessage(factory: MessageFactory, bookName: String) {
            val message = factory.createRawMessage {
                setParentEventId("eventId", bookName)
                setBody("body".toByteArray())
                metadata {
                    setSessionAlias("test")
                    setDirection(Direction.SECOND)
                    setSequence(1)
                    addSubsequence(2)
                    addSubsequence(3)
                    setBookName(bookName)
                    setTimestamp(Instant.now())
                    putProperty("propertyKey", "propertyValue")
                    setProtocol("protocol")
                }
            }
            println(message.toJson(false))
        }

        private fun testEvent(eventFactory: IEventFactory, bookName: String) {
            val event = Event.start(eventFactory)
                .bookName(bookName)
                .status(Event.Status.PASSED)
                .name("name")
                .type("type")
                .bodyData(EventUtils.createMessageBean("bodyData"))
                .messageID(MessageID.newBuilder().build())
                .toProto(null)
            println(event.toJson(false))
        }
    }
}
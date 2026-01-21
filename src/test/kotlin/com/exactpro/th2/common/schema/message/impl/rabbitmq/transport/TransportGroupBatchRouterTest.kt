/*
 * Copyright 2023-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.transport

import com.exactpro.th2.common.event.bean.BaseTest.*
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.configuration.MqRouterFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterOperation
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.configuration.GlobalNotificationConfiguration
import com.exactpro.th2.common.schema.message.impl.context.DefaultMessageRouterContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.PublishConnectionManager
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConsumeConnectionManager
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.TransportGroupBatchRouter.Companion.TRANSPORT_GROUP_ATTRIBUTE
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.kotlin.*
import java.time.Instant

class TransportGroupBatchRouterTest {
    private val connectionConfiguration = ConnectionManagerConfiguration()
    private val managerMonitor: ExclusiveSubscriberMonitor = mock { }
    private val publishConnectionManager: PublishConnectionManager = mock {
        on { configuration }.thenReturn(connectionConfiguration)
    }
    private val consumeConnectionManager: ConsumeConnectionManager = mock {
        on { configuration }.thenReturn(connectionConfiguration)
        on { basicConsume(any(), any(), any()) }.thenReturn(managerMonitor)
    }

    @Nested
    inner class Publishing {
        private val router = createRouter(
            mapOf(
                "test-pine" to QueueConfiguration(
                    routingKey = "",
                    queue = "subscribe",
                    exchange = "test-exchange",
                    attributes = listOf("subscribe", TRANSPORT_GROUP_ATTRIBUTE)
                ),
                "test-pin1" to QueueConfiguration(
                    routingKey = "test",
                    queue = "",
                    exchange = "test-exchange",
                    attributes = listOf("publish", TRANSPORT_GROUP_ATTRIBUTE),
                    filters = listOf(
                        MqRouterFilterConfiguration(
                            metadata = listOf(
                                FieldFilterConfiguration(
                                    fieldName = "message_type",
                                    expectedValue = "test-message",
                                    operation = FieldFilterOperation.EQUAL
                                )
                            )
                        )
                    )
                ),
                "test-pin2" to QueueConfiguration(
                    routingKey = "test2",
                    queue = "",
                    exchange = "test-exchange",
                    attributes = listOf("publish", TRANSPORT_GROUP_ATTRIBUTE, "test"),
                    filters = listOf(
                        MqRouterFilterConfiguration(
                            metadata = listOf(
                                FieldFilterConfiguration(
                                    fieldName = "message_type",
                                    expectedValue = "test-message",
                                    operation = FieldFilterOperation.EQUAL
                                )
                            )
                        )
                    )
                )
            )
        )

        @Test
        fun `publishes message group batch with metadata`() {
            val batch = createGroupBatch("test-message")

            router.send(batch, "test")

            val captor = argumentCaptor<ByteArray>()
            verify(publishConnectionManager).basicPublish(eq("test-exchange"), eq("test2"), anyOrNull(), captor.capture())
            val publishedBytes = captor.firstValue
            assertArrayEquals(batch.toByteArray(), publishedBytes) {
                "Unexpected batch published: ${MessageGroupBatch.parseFrom(publishedBytes)}"
            }
        }

        @Test
        fun `does not publish anything if all messages are filtered`() {
            router.send(createGroupBatch("test-message1"))

            verify(publishConnectionManager, never()).basicPublish(any(), any(), anyOrNull(), any())
        }

        @Test
        fun `publishes to the correct pin according to attributes`() {
            val batch = createGroupBatch("test-message")
            router.send(batch, "test")

            val captor = argumentCaptor<ByteArray>()
            verify(publishConnectionManager).basicPublish(eq("test-exchange"), eq("test2"), anyOrNull(), captor.capture())
            val publishedBytes = captor.firstValue
            assertArrayEquals(batch.toByteArray(), publishedBytes) {
                "Unexpected batch published: ${MessageGroupBatch.parseFrom(publishedBytes)}"
            }
        }

        @Test
        fun `reports about extra pins matches the publication`() {
            assertThrows(IllegalStateException::class.java) {
                router.send(createGroupBatch("test-message"))
            }.apply {
                Assertions.assertEquals(
                    "Found incorrect number of pins [test-pin1, test-pin2] to the send operation by attributes [publish, $TRANSPORT_GROUP_ATTRIBUTE] and filters, expected 1, actual 2",
                    message
                )
            }
        }

        @Test
        fun `reports about no pins matches the publication`() {
            assertThrows(IllegalStateException::class.java) {
                router.send(TRANSPORT_BATCH, "unexpected")
            }.apply {
                Assertions.assertEquals(
                    "Found incorrect number of pins [] to the send operation by attributes [unexpected, publish, $TRANSPORT_GROUP_ATTRIBUTE] and filters, expected 1, actual 0",
                    message
                )
            }
        }

        @Test
        fun `publishes to all correct pin according to attributes`() {
            val batch = createGroupBatch("test-message")
            router.sendAll(batch)

            val captor = argumentCaptor<ByteArray>()
            verify(publishConnectionManager).basicPublish(eq("test-exchange"), eq("test"), anyOrNull(), captor.capture())
            verify(publishConnectionManager).basicPublish(eq("test-exchange"), eq("test2"), anyOrNull(), captor.capture())
            val originalBytes = batch.toByteArray()
            Assertions.assertAll(
                Executable {
                    val publishedBytes = captor.firstValue
                    assertArrayEquals(originalBytes, publishedBytes) {
                        "Unexpected batch published: ${MessageGroupBatch.parseFrom(publishedBytes)}"
                    }
                },
                Executable {
                    val publishedBytes = captor.secondValue
                    assertArrayEquals(originalBytes, publishedBytes) {
                        "Unexpected batch published: ${MessageGroupBatch.parseFrom(publishedBytes)}"
                    }
                }
            )
        }
    }

    @Nested
    inner class Subscribing {
        private val router = createRouter(
            mapOf(
                "test" to QueueConfiguration(
                    routingKey = "publish",
                    queue = "",
                    exchange = "test-exchange",
                    attributes = listOf("publish", TRANSPORT_GROUP_ATTRIBUTE, "test")
                ),
                "test1" to QueueConfiguration(
                    routingKey = "",
                    queue = "queue1",
                    exchange = "test-exchange",
                    attributes = listOf("subscribe", TRANSPORT_GROUP_ATTRIBUTE, "1")
                ),
                "test2" to QueueConfiguration(
                    routingKey = "",
                    queue = "queue2",
                    exchange = "test-exchange",
                    attributes = listOf("subscribe", TRANSPORT_GROUP_ATTRIBUTE, "2")
                )
            )
        )

        @Test
        fun `publishes message group batch with metadata`() {
            val batch = createGroupBatch("test-message")
            router.send(batch, "test")

            val captor = argumentCaptor<ByteArray>()
            verify(publishConnectionManager).basicPublish(eq("test-exchange"), eq("publish"), anyOrNull(), captor.capture())
            val publishedBytes = captor.firstValue
            assertArrayEquals(batch.toByteArray(), publishedBytes) {
                "Unexpected batch published: ${MessageGroupBatch.parseFrom(publishedBytes)}"
            }
        }

        @Test
        fun `reports if more that one queue matches`() {
            assertThrows(IllegalStateException::class.java) { router.subscribe(mock { }) }
                .apply {
                    Assertions.assertEquals(
                        "Found incorrect number of pins [test1, test2] to subscribe operation by attributes [subscribe, $TRANSPORT_GROUP_ATTRIBUTE] and filters, expected 1, actual 2",
                        message
                    )
                }
        }

        @Test
        fun `reports if no queue matches`() {
            Assertions.assertAll(
                Executable {
                    assertThrows(IllegalStateException::class.java) {
                        router.subscribe(
                            mock { },
                            "unexpected"
                        )
                    }
                        .apply {
                            Assertions.assertEquals(
                                "Found incorrect number of pins [] to subscribe operation by attributes [unexpected, subscribe, $TRANSPORT_GROUP_ATTRIBUTE] and filters, expected 1, actual 0",
                                message
                            )
                        }
                },
                Executable {
                    assertThrows(IllegalStateException::class.java) {
                        router.subscribeAll(
                            mock { },
                            "unexpected"
                        )
                    }
                        .apply {
                            Assertions.assertEquals(
                                "Found incorrect number of pins [] to subscribe all operation by attributes [unexpected, subscribe, $TRANSPORT_GROUP_ATTRIBUTE] and filters, expected 1 or more, actual 0",
                                message
                            )
                        }
                }
            )
        }
    }

    private fun createGroupBatch(messageType: String) = GroupBatch(
        BOOK_NAME,
        SESSION_GROUP,
        mutableListOf(
            MessageGroup(
                mutableListOf(
                    ParsedMessage(
                        MessageId.builder()
                            .setSessionAlias(SESSION_ALIAS)
                            .setDirection(Direction.INCOMING)
                            .setSequence(1)
                            .setTimestamp(Instant.now())
                            .build(),
                        type = messageType
                    )
                )
            )
        )
    )

    private fun createRouter(pins: Map<String, QueueConfiguration>): MessageRouter<GroupBatch> =
        TransportGroupBatchRouter().apply {
            init(
                DefaultMessageRouterContext(
                    publishConnectionManager,
                    consumeConnectionManager,
                    mock { },
                    MessageRouterConfiguration(pins, GlobalNotificationConfiguration()),
                    BOX_CONFIGURATION
                )
            )
        }

    companion object {
        private val TRANSPORT_BATCH = GroupBatch(
            BOOK_NAME,
            SESSION_GROUP,
            mutableListOf(
                MessageGroup(
                    mutableListOf(
                        RawMessage(
                            MessageId.builder()
                                .setSessionAlias(SESSION_ALIAS)
                                .setDirection(Direction.INCOMING)
                                .setSequence(1)
                                .setTimestamp(Instant.now())
                                .build(),
                            body = Unpooled.wrappedBuffer(byteArrayOf(1, 2, 3))
                        )
                    )
                )
            )
        )
    }
}
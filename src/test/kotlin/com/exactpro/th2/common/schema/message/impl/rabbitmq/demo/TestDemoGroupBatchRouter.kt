/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.demo

import com.exactpro.th2.common.event.bean.BaseTest.BOOK_NAME
import com.exactpro.th2.common.event.bean.BaseTest.BOX_CONFIGURATION
import com.exactpro.th2.common.event.bean.BaseTest.SESSION_ALIAS
import com.exactpro.th2.common.event.bean.BaseTest.SESSION_GROUP
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.configuration.GlobalNotificationConfiguration
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.context.DefaultMessageRouterContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify

class TestDemoGroupBatchRouter {
    private val connectionConfiguration = ConnectionManagerConfiguration()
    private val monitor: ExclusiveSubscriberMonitor = mock { }
    private val connectionManager: ConnectionManager = mock {
        on { configuration }.thenReturn(connectionConfiguration)
        on { basicConsume(any(), any(), any()) }.thenReturn(monitor)
    }

    @Nested
    inner class Publishing {
        private val router = createRouter(
            mapOf(
                "test-pine" to QueueConfiguration(
                    routingKey = "",
                    queue = "subscribe",
                    exchange = "test-exchange",
                    attributes = listOf("subscribe")
                ),
                "test-pin1" to QueueConfiguration(
                    routingKey = "test",
                    queue = "",
                    exchange = "test-exchange",
                    attributes = listOf("publish"),
                ),
                "test-pin2" to QueueConfiguration(
                    routingKey = "test2",
                    queue = "",
                    exchange = "test-exchange",
                    attributes = listOf("publish", "test"),
                )
            )
        )

        @Test
        fun `publishes to the correct pin according to attributes`() {
            val batch = DemoGroupBatch(
                BOOK_NAME,
                SESSION_GROUP,
                listOf(
                    DemoMessageGroup(
                        listOf(
                            DemoRawMessage(
                                DemoMessageId(BOOK_NAME, SESSION_GROUP, SESSION_ALIAS),
                                body = byteArrayOf(1, 2, 3)
                            )
                        )
                    )
                )
            )
            router.send(batch, "test")


            val captor = argumentCaptor<ByteArray>()
            verify(connectionManager).basicPublish(eq("test-exchange"), eq("test2"), anyOrNull(), captor.capture())
            val publishedBytes = captor.firstValue
            assertArrayEquals(batch.toByteArray(), publishedBytes) {
                "Unexpected batch published: ${MessageGroupBatch.parseFrom(publishedBytes)}"
            }
        }

        @Test
        fun `reports about extra pins matches the publication`() {
            Assertions.assertThrows(IllegalStateException::class.java) {
                router.send(DEMO_MESSAGE_BATCH)
            }.apply {
                Assertions.assertEquals(
                    "Found incorrect number of pins [test-pin1, test-pin2] to the send operation by attributes [publish] and filters, expected 1, actual 2",
                    message
                )
            }
        }

        @Test
        fun `reports about no pins matches the publication`() {
            Assertions.assertThrows(IllegalStateException::class.java) {
                router.send(DEMO_MESSAGE_BATCH, "unexpected")
            }.apply {
                Assertions.assertEquals(
                    "Found incorrect number of pins [] to the send operation by attributes [unexpected, publish] and filters, expected 1, actual 0",
                    message
                )
            }
        }

        @Test
        fun `publishes to all correct pin according to attributes`() {
            router.sendAll(DEMO_MESSAGE_BATCH)

            val captor = argumentCaptor<ByteArray>()
            verify(connectionManager).basicPublish(eq("test-exchange"), eq("test"), anyOrNull(), captor.capture())
            verify(connectionManager).basicPublish(eq("test-exchange"), eq("test2"), anyOrNull(), captor.capture())
            val originalBytes = DEMO_MESSAGE_BATCH.toByteArray()
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
                    attributes = listOf("publish", "test")
                ),
                "test1" to QueueConfiguration(
                    routingKey = "",
                    queue = "queue1",
                    exchange = "test-exchange",
                    attributes = listOf("subscribe", "1")
                ),
                "test2" to QueueConfiguration(
                    routingKey = "",
                    queue = "queue2",
                    exchange = "test-exchange",
                    attributes = listOf("subscribe", "2")
                )
            )
        )

        @Test
        fun `subscribes to correct queue`() {
            val monitor = router.subscribe(mock { }, "1")
            Assertions.assertNotNull(monitor) { "monitor must not be null" }

            verify(connectionManager).basicConsume(eq("queue1"), any(), any())
        }
        @Test
        fun `subscribes with manual ack to correct queue`() {
            val monitor = router.subscribeWithManualAck(mock { }, "1")
            Assertions.assertNotNull(monitor) { "monitor must not be null" }

            verify(connectionManager).basicConsume(eq("queue1"), any(), any())
        }

        @Test
        fun `subscribes to all matched queues`() {
            val monitor = router.subscribeAll(mock { })
            Assertions.assertNotNull(monitor) { "monitor must not be null" }

            verify(connectionManager).basicConsume(eq("queue1"), any(), any())
            verify(connectionManager).basicConsume(eq("queue2"), any(), any())
        }

        @Test
        fun `reports if more that one queue matches`() {
            Assertions.assertThrows(IllegalStateException::class.java) { router.subscribe(mock { }) }
                .apply {
                    Assertions.assertEquals(
                        "Found incorrect number of pins [test1, test2] to subscribe operation by attributes [subscribe] and filters, expected 1, actual 2",
                        message
                    )
                }
        }

        @Test
        fun `reports if no queue matches`() {
            Assertions.assertAll(
                Executable {
                    Assertions.assertThrows(IllegalStateException::class.java) { router.subscribe(mock { }, "unexpected") }
                        .apply {
                            Assertions.assertEquals(
                                "Found incorrect number of pins [] to subscribe operation by attributes [unexpected, subscribe] and filters, expected 1, actual 0",
                                message
                            )
                        }
                },
                Executable {
                    Assertions.assertThrows(IllegalStateException::class.java) { router.subscribeAll(mock { }, "unexpected") }
                        .apply {
                            Assertions.assertEquals(
                                "Found incorrect number of pins [] to subscribe all operation by attributes [unexpected, subscribe] and filters, expected 1 or more, actual 0",
                                message
                            )
                        }
                }
            )
        }
    }

    private fun createRouter(pins: Map<String, QueueConfiguration>): MessageRouter<DemoGroupBatch> =
        DemoMessageBatchRouter().apply {
            init(
                DefaultMessageRouterContext(
                    connectionManager,
                    mock { },
                    MessageRouterConfiguration(pins, GlobalNotificationConfiguration()),
                    BOX_CONFIGURATION
                )
            )
        }

    companion object {
        private val DEMO_MESSAGE_BATCH = DemoGroupBatch(
            BOOK_NAME,
            SESSION_GROUP,
            listOf(
                DemoMessageGroup(
                    listOf(
                        DemoRawMessage(
                            DemoMessageId(BOOK_NAME, SESSION_GROUP, SESSION_ALIAS),
                            body = byteArrayOf(1, 2, 3)
                        )
                    )
                )
            )
        )
    }
}

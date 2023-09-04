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
package com.exactpro.th2.common.schema.message.impl.rabbitmq

import com.exactpro.th2.common.event.bean.BaseTest
import com.exactpro.th2.common.schema.message.ConfirmationListener
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.MessageSender
import com.exactpro.th2.common.schema.message.MessageSubscriber
import com.exactpro.th2.common.schema.message.configuration.GlobalNotificationConfiguration
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.context.DefaultMessageRouterContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.TransportGroupBatchRouter
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertNotEquals

private const val TEST_EXCLUSIVE_QUEUE = "test-exclusive-queue"

class AbstractRabbitRouterTest {
    private val connectionConfiguration = ConnectionManagerConfiguration()
    private val managerMonitor: ExclusiveSubscriberMonitor = mock { }
    private val connectionManager: ConnectionManager = mock {
        on { configuration }.thenReturn(connectionConfiguration)
        on { basicConsume(any(), any(), any()) }.thenReturn(managerMonitor)
        on { queueDeclare() }.thenAnswer { "$TEST_EXCLUSIVE_QUEUE-${exclusiveQueueCounter.incrementAndGet()}" }
    }

    @Nested
    inner class Subscribing {
        private val router = createRouter(
            mapOf(
                "test" to QueueConfiguration(
                    routingKey = "publish",
                    queue = "",
                    exchange = "test-exchange",
                    attributes = listOf("publish", TransportGroupBatchRouter.TRANSPORT_GROUP_ATTRIBUTE, "test")
                ),
                "test1" to QueueConfiguration(
                    routingKey = "",
                    queue = "queue1",
                    exchange = "test-exchange",
                    attributes = listOf("subscribe", TransportGroupBatchRouter.TRANSPORT_GROUP_ATTRIBUTE, "1")
                ),
                "test2" to QueueConfiguration(
                    routingKey = "",
                    queue = "queue2",
                    exchange = "test-exchange",
                    attributes = listOf("subscribe", TransportGroupBatchRouter.TRANSPORT_GROUP_ATTRIBUTE, "2")
                )
            )
        )

        @AfterEach
        fun afterEach() {
            verifyNoMoreInteractions(connectionManager)
            verifyNoMoreInteractions(managerMonitor)
        }

        @Test
        fun `subscribes to correct queue`() {
            val monitor = router.subscribe(mock { }, "1")
            assertNotNull(monitor) { "monitor must not be null" }

            verify(connectionManager).basicConsume(eq("queue1"), any(), any())
        }

        @Test
        fun `subscribes with manual ack to correct queue`() {
            val monitor = router.subscribeWithManualAck(mock { }, "1")
            assertNotNull(monitor) { "monitor must not be null" }

            verify(connectionManager).basicConsume(eq("queue1"), any(), any())
        }

        @Test
        fun `subscribes to all matched queues`() {
            val monitor = router.subscribeAll(mock { })
            assertNotNull(monitor) { "monitor must not be null" }

            verify(connectionManager).basicConsume(eq("queue1"), any(), any())
            verify(connectionManager).basicConsume(eq("queue2"), any(), any())
        }

        @Test
        fun `unsubscribe after subscribe`() {
            val monitor = router.subscribe(mock { }, "1")
            clearInvocations(connectionManager)
            clearInvocations(managerMonitor)

            monitor.unsubscribe()
            verify(managerMonitor).unsubscribe()
        }

        @Test
        fun `unsubscribe after subscribe with manual ack`() {
            val monitor = router.subscribeWithManualAck(mock { }, "1")
            clearInvocations(connectionManager)
            clearInvocations(managerMonitor)

            monitor.unsubscribe()
            verify(managerMonitor).unsubscribe()
        }

        @Test
        fun `unsubscribe after subscribe to exclusive queue`() {
            val monitor = router.subscribeExclusive(mock { })
            clearInvocations(connectionManager)
            clearInvocations(managerMonitor)

            monitor.unsubscribe()
            verify(managerMonitor).unsubscribe()
        }

        @Test
        fun `subscribes after unsubscribe`() {
            router.subscribe(mock { }, "1")
                .unsubscribe()
            clearInvocations(connectionManager)
            clearInvocations(managerMonitor)

            val monitor = router.subscribe(mock { }, "1")
            assertNotNull(monitor) { "monitor must not be null" }
            verify(connectionManager).basicConsume(eq("queue1"), any(), any())
        }

        @Test
        fun `subscribes with manual ack after unsubscribe`() {
            router.subscribeWithManualAck(mock { }, "1")
                .unsubscribe()
            clearInvocations(connectionManager)
            clearInvocations(managerMonitor)

            val monitor = router.subscribeWithManualAck(mock { }, "1")
            assertNotNull(monitor) { "monitor must not be null" }
            verify(connectionManager).basicConsume(eq("queue1"), any(), any())
        }

        @Test
        fun `subscribes when subscribtion active`() {
            router.subscribe(mock { }, "1")
            clearInvocations(connectionManager)
            clearInvocations(managerMonitor)

            assertThrows(RuntimeException::class.java) { router.subscribe(mock { }, "1") }
        }

        @Test
        fun `subscribes with manual ack when subscribtion active`() {
            router.subscribeWithManualAck(mock { }, "1")
            clearInvocations(connectionManager)
            clearInvocations(managerMonitor)

            assertThrows(RuntimeException::class.java) { router.subscribeWithManualAck(mock { }, "1") }
        }

        @Test
        fun `subscribes to exclusive queue when subscribtion active`() {
            val monitorA = router.subscribeExclusive(mock { })
            val monitorB = router.subscribeExclusive(mock { })

            verify(connectionManager, times(2)).queueDeclare()
            verify(connectionManager, times(2)).basicConsume(argThat { matches(Regex("$TEST_EXCLUSIVE_QUEUE-\\d+")) }, any(), any())
            assertNotEquals(monitorA.queue, monitorB.queue)
        }

        @Test
        fun `reports if more that one queue matches`() {
            assertThrows(IllegalStateException::class.java) { router.subscribe(mock { }) }
                .apply {
                    assertEquals(
                        "Found incorrect number of pins [test1, test2] to subscribe operation by attributes [subscribe] and filters, expected 1, actual 2",
                        message
                    )
                }
        }

        @Test
        fun `reports if no queue matches`() {
            assertAll(
                Executable {
                    assertThrows(IllegalStateException::class.java) {
                        router.subscribe(
                            mock { },
                            "unexpected"
                        )
                    }
                        .apply {
                            assertEquals(
                                "Found incorrect number of pins [] to subscribe operation by attributes [unexpected, subscribe] and filters, expected 1, actual 0",
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
                            assertEquals(
                                "Found incorrect number of pins [] to subscribe all operation by attributes [unexpected, subscribe] and filters, expected 1 or more, actual 0",
                                message
                            )
                        }
                }
            )
        }
    }

    private class TestSender(
        connectionManager: ConnectionManager,
        exchangeName: String,
        routingKey: String,
        th2Pin: String,
        bookName: BookName,
    ) : AbstractRabbitSender<String>(
        connectionManager,
        exchangeName,
        routingKey,
        th2Pin,
        "test-string",
        bookName
    ) {
        override fun toShortTraceString(value: String): String = value

        override fun toShortDebugString(value: String): String = value

        override fun valueToBytes(value: String): ByteArray = value.toByteArray()
    }

    private class TestSubscriber(
        connectionManager: ConnectionManager,
        queue: String,
        th2Pin: String,
        listener: ConfirmationListener<String>
    ) : AbstractRabbitSubscriber<String>(
        connectionManager,
        queue,
        th2Pin,
        "test-string",
        listener
    ) {
        override fun valueFromBytes(body: ByteArray): String = String(body)

        override fun filter(value: String): String = value

        override fun toShortDebugString(value: String): String = value

        override fun toShortTraceString(value: String): String = value
    }

    private class TestRouter : AbstractRabbitRouter<String>() {
        override fun splitAndFilter(message: String, pinConfiguration: PinConfiguration, pinName: PinName): String {
            return message
        }

        override fun createSender(
            pinConfig: PinConfiguration,
            pinName: PinName,
            bookName: BookName
        ): MessageSender<String> = TestSender(
            connectionManager,
            pinConfig.exchange,
            pinConfig.routingKey,
            pinName,
            bookName
        )

        override fun String.toErrorString(): String = "Error $this"

        override fun createSubscriber(
            pinConfig: PinConfiguration,
            pinName: PinName,
            listener: ConfirmationListener<String>
        ): MessageSubscriber = TestSubscriber(
            connectionManager,
            pinConfig.queue,
            pinName,
            listener
        )

    }

    private fun createRouter(pins: Map<String, QueueConfiguration>): TestRouter =
        TestRouter().apply {
            init(
                DefaultMessageRouterContext(
                    connectionManager,
                    mock { },
                    MessageRouterConfiguration(pins, GlobalNotificationConfiguration()),
                    BaseTest.BOX_CONFIGURATION
                )
            )
        }

    companion object {
        private val exclusiveQueueCounter = AtomicInteger(0)
    }
}
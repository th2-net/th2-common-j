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
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.configuration.GlobalNotificationConfiguration
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.context.DefaultMessageRouterContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.exactpro.th2.common.schema.message.impl.rabbitmq.custom.RabbitCustomRouter
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
    private val publishConnectionManager: ConnectionManager = mock {
        on { configuration }.thenReturn(connectionConfiguration)
        on { basicConsume(any(), any(), any()) }.thenReturn(managerMonitor)
        on { queueDeclare() }.thenAnswer { "$TEST_EXCLUSIVE_QUEUE-${exclusiveQueueCounter.incrementAndGet()}" }
    }

    private val consumeConnectionManager: ConnectionManager = mock {
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

        @AfterEach
        fun afterEach() {
            verifyNoMoreInteractions(publishConnectionManager)
            verifyNoMoreInteractions(consumeConnectionManager)
            verifyNoMoreInteractions(managerMonitor)
        }

        @Test
        fun `subscribes to correct queue`() {
            val monitor = router.subscribe(mock { }, "1")
            assertNotNull(monitor) { "monitor must not be null" }

            verify(consumeConnectionManager).basicConsume(eq("queue1"), any(), any())
        }

        @Test
        fun `subscribes with manual ack to correct queue`() {
            val monitor = router.subscribeWithManualAck(mock { }, "1")
            assertNotNull(monitor) { "monitor must not be null" }

            verify(consumeConnectionManager).basicConsume(eq("queue1"), any(), any())
        }

        @Test
        fun `subscribes to all matched queues`() {
            val monitor = router.subscribeAll(mock { })
            assertNotNull(monitor) { "monitor must not be null" }

            verify(consumeConnectionManager).basicConsume(eq("queue1"), any(), any())
            verify(consumeConnectionManager).basicConsume(eq("queue2"), any(), any())
        }

        @Test
        fun `unsubscribe after subscribe`() {
            val monitor = router.subscribe(mock { }, "1")
            clearInvocations(consumeConnectionManager)
            clearInvocations(managerMonitor)

            monitor.unsubscribe()
            verify(managerMonitor).unsubscribe()
        }

        @Test
        fun `unsubscribe after subscribe with manual ack`() {
            val monitor = router.subscribeWithManualAck(mock { }, "1")
            clearInvocations(consumeConnectionManager)
            clearInvocations(managerMonitor)

            monitor.unsubscribe()
            verify(managerMonitor).unsubscribe()
        }

        @Test
        fun `unsubscribe after subscribe to exclusive queue`() {
            val monitor = router.subscribeExclusive(mock { })
            clearInvocations(consumeConnectionManager)
            clearInvocations(managerMonitor)

            monitor.unsubscribe()
            verify(managerMonitor).unsubscribe()
        }

        @Test
        fun `subscribes after unsubscribe`() {
            router.subscribe(mock { }, "1")
                .unsubscribe()
            clearInvocations(consumeConnectionManager)
            clearInvocations(managerMonitor)

            val monitor = router.subscribe(mock { }, "1")
            assertNotNull(monitor) { "monitor must not be null" }
            verify(consumeConnectionManager).basicConsume(eq("queue1"), any(), any())
        }

        @Test
        fun `subscribes with manual ack after unsubscribe`() {
            router.subscribeWithManualAck(mock { }, "1")
                .unsubscribe()
            clearInvocations(consumeConnectionManager)
            clearInvocations(managerMonitor)

            val monitor = router.subscribeWithManualAck(mock { }, "1")
            assertNotNull(monitor) { "monitor must not be null" }
            verify(consumeConnectionManager).basicConsume(eq("queue1"), any(), any())
        }

        @Test
        fun `subscribes when subscribtion active`() {
            router.subscribe(mock { }, "1")
            clearInvocations(consumeConnectionManager)
            clearInvocations(managerMonitor)

            assertThrows(RuntimeException::class.java) { router.subscribe(mock { }, "1") }
        }

        @Test
        fun `subscribes with manual ack when subscription active`() {
            router.subscribeWithManualAck(mock { }, "1")
            clearInvocations(consumeConnectionManager)
            clearInvocations(managerMonitor)

            assertThrows(RuntimeException::class.java) { router.subscribeWithManualAck(mock { }, "1") }
        }

        @Test
        fun `subscribes to exclusive queue when subscribtion active`() {
            val monitorA = router.subscribeExclusive(mock { })
            val monitorB = router.subscribeExclusive(mock { })

            verify(consumeConnectionManager, times(2)).queueDeclare()
            verify(consumeConnectionManager, times(2)).basicConsume(
                argThat { matches(Regex("$TEST_EXCLUSIVE_QUEUE-\\d+")) },
                any(),
                any()
            )
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

    private fun createRouter(pins: Map<String, QueueConfiguration>): RabbitCustomRouter<String> =
        RabbitCustomRouter(
            "test-custom-tag",
            arrayOf("test-label"),
            TestMessageConverter()
        ).apply {
            init(
                DefaultMessageRouterContext(
                    publishConnectionManager,
                    consumeConnectionManager,
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
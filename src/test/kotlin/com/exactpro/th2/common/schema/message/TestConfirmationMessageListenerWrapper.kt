/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify

class TestConfirmationMessageListenerWrapper {
    @Test
    fun `calls confirmation when requested`() {
        val listener = ConfirmationMessageListener.wrap<Any> { _, _ -> }

        mock<ManualAckDeliveryCallback.Confirmation> {}.also {
            listener.handle("", 2, it)
            verify(it, never()).confirm()
        }
    }

    @Test
    fun `calls confirmation when requested and method throw an exception`() {
        val listener = ConfirmationMessageListener.wrap<Any> { _, _ -> error("test") }

        mock<ManualAckDeliveryCallback.Confirmation> {}.also {
            assertThrows(IllegalStateException::class.java) { listener.handle("", 2, it) }.apply {
                assertEquals("test", message)
            }
            verify(it, never()).confirm()
        }
    }


}
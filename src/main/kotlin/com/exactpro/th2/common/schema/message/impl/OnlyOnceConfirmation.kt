/*
 * Copyright 2022-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl

import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.atomic.AtomicBoolean

class OnlyOnceConfirmation private constructor(
    private val id: String,
    private val delegate: ManualAckDeliveryCallback.Confirmation
) : ManualAckDeliveryCallback.Confirmation {
    private val called = AtomicBoolean()

    override fun confirm() {
        if (called.compareAndSet(false, true)) {
            delegate.confirm()
        } else {
            LOGGER.warn { "Confirmation or rejection '$id' invoked more that one time" }
        }
    }

    override fun reject() {
        if (called.compareAndSet(false, true)) {
            delegate.reject()
        } else {
            LOGGER.warn { "Confirmation or rejection '$id' invoked more that one time" }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        @JvmStatic
        fun wrap(id: String, confirmation: ManualAckDeliveryCallback.Confirmation): ManualAckDeliveryCallback.Confirmation =
            OnlyOnceConfirmation(id, confirmation)
    }
}
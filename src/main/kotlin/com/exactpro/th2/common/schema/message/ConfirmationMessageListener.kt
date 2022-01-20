/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

interface ConfirmationMessageListener<T> {
    /**
     * If returns `true` that means the listener handles confirmation by himself
     * and no auto-confirmation should be performed.
     *
     * If returns `false` the confirmation should be performed automatically when method [handle] returns
     */
    val manualConfirmation: Boolean

    @Throws(Exception::class)
    fun handle(consumerTag: String, message: T, confirmation: ManualAckDeliveryCallback.Confirmation)

    fun onClose() {}

    companion object {
        @JvmStatic
        @JvmOverloads
        fun <T> wrap(listener: MessageListener<T>, confirm: Boolean = false): ConfirmationMessageListener<T> = DelegateListener(listener, confirm)
    }
}

private class DelegateListener<T>(
    private val delegate: MessageListener<T>,
    private val confirm: Boolean
) : ConfirmationMessageListener<T> {
    override val manualConfirmation: Boolean
        get() = false

    override fun handle(consumerTag: String, message: T, confirmation: ManualAckDeliveryCallback.Confirmation) {
        try {
            delegate.handler(consumerTag, message)
        } finally {
            // do not call confirmation if we were tolled so
            if (confirm) {
                confirmation.confirm()
            }
        }
    }

    override fun onClose() {
        delegate.onClose()
    }

    override fun toString(): String = "Delegate($delegate)"
}
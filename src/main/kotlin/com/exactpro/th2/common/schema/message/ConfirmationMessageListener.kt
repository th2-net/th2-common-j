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

    @Throws(Exception::class)
    fun handle(consumerTag: String, message: T, confirmation: ManualAckDeliveryCallback.Confirmation)

    fun onClose() {}

    companion object {
        @JvmStatic
        fun <T> wrap(listener: MessageListener<T>): ConfirmationMessageListener<T> = DelegateListener(listener)

        /**
         * @return `true` if the listener uses manual acknowledgment
         */
        @JvmStatic
        fun isManual(listener: ConfirmationMessageListener<*>): Boolean = listener is ManualConfirmationListener<*>
    }
}

/**
 * The interface marker that indicates that acknowledge will be manually invoked by the listener itself
 */
interface ManualConfirmationListener<T> : ConfirmationMessageListener<T> {
    /**
     * The listener must invoke the [confirmation] callback once it has processed the [message]
     * @see ConfirmationMessageListener.handle
     */
    override fun handle(consumerTag: String, message: T, confirmation: ManualAckDeliveryCallback.Confirmation)
}

private class DelegateListener<T>(
    private val delegate: MessageListener<T>,
) : ConfirmationMessageListener<T> {

    override fun handle(consumerTag: String, message: T, confirmation: ManualAckDeliveryCallback.Confirmation) {
        delegate.handle(consumerTag, message)
    }

    override fun onClose() {
        delegate.onClose()
    }

    override fun toString(): String = "Delegate($delegate)"
}
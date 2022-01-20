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

import com.rabbitmq.client.Delivery
import java.io.IOException

interface ManualAckDeliveryCallback {
    /**
     * Called when a delivery from queue is received
     * @param consumerTag the _consumer_ tag associated with the consumer
     * @param delivery the delivered message
     * @param confirmProcessed the action that should be invoked when the message can be considered as processed
     */
    @Throws(IOException::class)
    fun handle(consumerTag: String, delivery: Delivery, confirmProcessed: Confirmation)

    @FunctionalInterface
    interface Confirmation {
        @Throws(IOException::class)
        fun confirm()
    }
}
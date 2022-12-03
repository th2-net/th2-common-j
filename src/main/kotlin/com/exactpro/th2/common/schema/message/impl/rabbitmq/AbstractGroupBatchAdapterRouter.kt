/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.ManualConfirmationListener
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.MessageRouterContext
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.exactpro.th2.common.schema.message.appendAttributes

abstract class AbstractGroupBatchAdapterRouter<T> : MessageRouter<T> {
    private lateinit var groupBatchRouter: MessageRouter<MessageGroupBatch>

    abstract fun getRequiredSendAttributes(): Set<String>

    abstract fun getRequiredSubscribeAttributes(): Set<String>

    protected abstract fun buildGroupBatch(batch: T): MessageGroupBatch

    protected abstract fun buildFromGroupBatch(groupBatch: MessageGroupBatch): T

    override fun init(context: MessageRouterContext) {}

    override fun init(context: MessageRouterContext, groupBatchRouter: MessageRouter<MessageGroupBatch>) {
        this.groupBatchRouter = groupBatchRouter
    }

    override fun subscribeExclusive(callback: MessageListener<T>): ExclusiveSubscriberMonitor {
        return groupBatchRouter.subscribeExclusive { deliveryMetadata: DeliveryMetadata, message: MessageGroupBatch ->
            callback.handle(deliveryMetadata, buildFromGroupBatch(message))
        }
    }

    override fun subscribe(callback: MessageListener<T>, vararg attributes: String): SubscriberMonitor {
        return groupBatchRouter.subscribe({ deliveryMetadata: DeliveryMetadata, message: MessageGroupBatch ->
                callback.handle(deliveryMetadata, buildFromGroupBatch(message))
            },
            *appendAttributes(*attributes) { getRequiredSubscribeAttributes() }.toTypedArray()
        )
    }

    override fun subscribeAll(callback: MessageListener<T>, vararg attributes: String): SubscriberMonitor {
        return groupBatchRouter.subscribeAll({ deliveryMetadata: DeliveryMetadata, message: MessageGroupBatch ->
                callback.handle(deliveryMetadata, buildFromGroupBatch(message))
            },
            *appendAttributes(*attributes) { getRequiredSubscribeAttributes() }.toTypedArray()
        )
    }

    override fun subscribeWithManualAck(
        callback: ManualConfirmationListener<T>,
        vararg queueAttr: String
    ): SubscriberMonitor {
        val listener =
            ManualConfirmationListener<MessageGroupBatch> { deliveryMetadata, message, confirmation ->
                callback.handle(deliveryMetadata, buildFromGroupBatch(message), confirmation)
            }

        return groupBatchRouter.subscribeWithManualAck(
            listener,
            *appendAttributes(*queueAttr) { getRequiredSubscribeAttributes() }.toTypedArray()
        )
    }

    override fun subscribeAllWithManualAck(
        callback: ManualConfirmationListener<T>,
        vararg queueAttr: String
    ): SubscriberMonitor {
        val listener =
            ManualConfirmationListener<MessageGroupBatch> { deliveryMetadata, message, confirmation ->
                callback.handle(deliveryMetadata, buildFromGroupBatch(message), confirmation)
            }

        return groupBatchRouter.subscribeAllWithManualAck(listener,
            *appendAttributes(*queueAttr) { getRequiredSubscribeAttributes() }.toTypedArray()
        )
    }

    override fun sendExclusive(queue: String, messageBatch: T) {
        groupBatchRouter.sendExclusive(
            queue,
            buildGroupBatch(messageBatch)
        )
    }

    override fun send(messageBatch: T, vararg attributes: String) {
        groupBatchRouter.send(
            buildGroupBatch(messageBatch),
            *appendAttributes(*attributes) { getRequiredSendAttributes() }.toTypedArray()
        )
    }

    override fun sendAll(messageBatch: T, vararg attributes: String) {
        groupBatchRouter.sendAll(
            buildGroupBatch(messageBatch),
            *appendAttributes(*attributes) { getRequiredSendAttributes() }.toTypedArray()
        )
    }

    override fun close() {
        groupBatchRouter.close()
    }
}
/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.factory

import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.schema.event.EventBatchRouter
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.schema.grpc.router.impl.DefaultGrpcRouter
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.NotificationRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.group.RabbitMessageGroupBatchRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.notification.NotificationEventBatchRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.parsed.RabbitParsedBatchRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.raw.RabbitRawBatchRouter
import java.nio.file.Path

data class FactorySettings @JvmOverloads constructor(
    var messageRouterParsedBatchClass: Class<out MessageRouter<MessageBatch>> = RabbitParsedBatchRouter::class.java,
    var messageRouterRawBatchClass: Class<out MessageRouter<RawMessageBatch>> = RabbitRawBatchRouter::class.java,
    var messageRouterMessageGroupBatchClass: Class<out MessageRouter<MessageGroupBatch>> = RabbitMessageGroupBatchRouter::class.java,
    var eventBatchRouterClass: Class<out MessageRouter<EventBatch>> = EventBatchRouter::class.java,
    var grpcRouterClass: Class<out GrpcRouter> = DefaultGrpcRouter::class.java,
    var notificationEventBatchRouterClass: Class<out NotificationRouter<EventBatch>> = NotificationEventBatchRouter::class.java,
    var variables: MutableMap<String, String> = HashMap(),
    var rabbitMQ: Path? = null,
    var routerMQ: Path? = null,
    var connectionManagerSettings: Path? = null,
    var grpc: Path? = null,
    var routerGRPC: Path? = null,
    var cradleConfidential: Path? = null,
    var cradleNonConfidential: Path? = null,
    var prometheus: Path? = null,
    var boxConfiguration: Path? = null,
    var custom: Path? = null,
    var dictionariesDir: Path? = null,
    var oldDictionariesDir: Path? = null
) {
    fun putVariable(key: String, value: String): String? {
        return variables.put(key, value)
    }
}
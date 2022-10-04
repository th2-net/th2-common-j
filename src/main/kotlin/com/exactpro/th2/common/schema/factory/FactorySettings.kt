/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.ConfigurationProvider
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.module.provider.FileConfigurationProvider
import com.exactpro.th2.common.schema.event.EventBatchRouter
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.schema.grpc.router.impl.DefaultGrpcRouter
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.group.RabbitMessageGroupBatchRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.parsed.RabbitParsedBatchRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.raw.RabbitRawBatchRouter
import java.nio.file.Path


data class FactorySettings @JvmOverloads constructor(
    var messageRouterParsedBatchClass: Class<out MessageRouter<MessageBatch>> = RabbitParsedBatchRouter::class.java,
    var messageRouterRawBatchClass: Class<out MessageRouter<RawMessageBatch>> = RabbitRawBatchRouter::class.java,
    var messageRouterMessageGroupBatchClass: Class<out MessageRouter<MessageGroupBatch>> = RabbitMessageGroupBatchRouter::class.java,
    var eventBatchRouterClass: Class<out MessageRouter<EventBatch>> = EventBatchRouter::class.java,
    var grpcRouterClass: Class<out GrpcRouter> = DefaultGrpcRouter::class.java,
//    var configurationProvider: ConfigurationProvider? = null,
    var configurationProviderClass: Class<out ConfigurationProvider> = FileConfigurationProvider::class.java,
    var configurationProviderArgs: Array<String> = emptyArray(),

    var rabbitMQ: Path? = null,
    var routerMQ: Path? = null,
    var connectionManagerSettings: Path? = null,
    var grpc: Path? = null,
    var routerGRPC: Path? = null,
    var prometheus: Path? = null,
    var boxConfiguration: Path? = null,
    var custom: Path? = null,
    @Deprecated("Will be removed in future releases") var dictionaryTypesDir: Path? = null,
    var dictionaryAliasesDir: Path? = null,
    @Deprecated("Will be removed in future releases") var oldDictionariesDir: Path? = null) {
    private val _variables: MutableMap<String, String> = HashMap()
    val variables: Map<String, String> = _variables

    fun putVariable(key: String, value: String): String? {
        return _variables.put(key, value)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as FactorySettings

        if (messageRouterParsedBatchClass != other.messageRouterParsedBatchClass) return false
        if (messageRouterRawBatchClass != other.messageRouterRawBatchClass) return false
        if (messageRouterMessageGroupBatchClass != other.messageRouterMessageGroupBatchClass) return false
        if (eventBatchRouterClass != other.eventBatchRouterClass) return false
        if (grpcRouterClass != other.grpcRouterClass) return false
        if (configurationProviderClass != other.configurationProviderClass) return false
        if (!configurationProviderArgs.contentEquals(other.configurationProviderArgs)) return false
        if (rabbitMQ != other.rabbitMQ) return false
        if (routerMQ != other.routerMQ) return false
        if (connectionManagerSettings != other.connectionManagerSettings) return false
        if (grpc != other.grpc) return false
        if (routerGRPC != other.routerGRPC) return false
        if (prometheus != other.prometheus) return false
        if (boxConfiguration != other.boxConfiguration) return false
        if (custom != other.custom) return false
        if (dictionaryTypesDir != other.dictionaryTypesDir) return false
        if (dictionaryAliasesDir != other.dictionaryAliasesDir) return false
        if (oldDictionariesDir != other.oldDictionariesDir) return false
        if (_variables != other._variables) return false
        if (variables != other.variables) return false

        return true
    }

    override fun hashCode(): Int {
        var result = messageRouterParsedBatchClass.hashCode()
        result = 31 * result + messageRouterRawBatchClass.hashCode()
        result = 31 * result + messageRouterMessageGroupBatchClass.hashCode()
        result = 31 * result + eventBatchRouterClass.hashCode()
        result = 31 * result + grpcRouterClass.hashCode()
        result = 31 * result + configurationProviderClass.hashCode()
        result = 31 * result + configurationProviderArgs.contentHashCode()
        result = 31 * result + (rabbitMQ?.hashCode() ?: 0)
        result = 31 * result + (routerMQ?.hashCode() ?: 0)
        result = 31 * result + (connectionManagerSettings?.hashCode() ?: 0)
        result = 31 * result + (grpc?.hashCode() ?: 0)
        result = 31 * result + (routerGRPC?.hashCode() ?: 0)
        result = 31 * result + (prometheus?.hashCode() ?: 0)
        result = 31 * result + (boxConfiguration?.hashCode() ?: 0)
        result = 31 * result + (custom?.hashCode() ?: 0)
        result = 31 * result + (dictionaryTypesDir?.hashCode() ?: 0)
        result = 31 * result + (dictionaryAliasesDir?.hashCode() ?: 0)
        result = 31 * result + (oldDictionariesDir?.hashCode() ?: 0)
        result = 31 * result + _variables.hashCode()
        result = 31 * result + variables.hashCode()
        return result
    }
}
/*
 * Copyright 2021-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.filter.strategy.impl

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.message.toJson
import com.google.protobuf.Message

object AnyMessageFilterStrategy : AbstractFilterStrategy<Message>() {

    override fun getFields(message: Message): MutableMap<String, String> {
        check(message is AnyMessage) { "Message is not an ${AnyMessage::class.qualifiedName}: ${message.toJson()}" }

        val result = HashMap<String, String>()

        when {
            message.hasMessage() -> {
                result.putAll(message.message.fieldsMap.mapValues { it.value.simpleValue })

                val metadata = message.message.metadata
                val sessionAlias = metadata.id.connectionId.sessionAlias
                val sessionGroup = metadata.id.connectionId.sessionGroup
                result.putAll(metadata.propertiesMap)
                result[AbstractTh2MsgFilterStrategy.BOOK_KEY] = metadata.id.bookName
                result[AbstractTh2MsgFilterStrategy.SESSION_GROUP_KEY] = sessionGroup.ifEmpty { sessionAlias }
                result[AbstractTh2MsgFilterStrategy.SESSION_ALIAS_KEY] = sessionAlias
                result[AbstractTh2MsgFilterStrategy.MESSAGE_TYPE_KEY] = metadata.messageType
                result[AbstractTh2MsgFilterStrategy.DIRECTION_KEY] = metadata.id.direction.name
                result[AbstractTh2MsgFilterStrategy.PROTOCOL_KEY] = metadata.protocol
            }
            message.hasRawMessage() -> {
                val metadata = message.rawMessage.metadata
                val sessionAlias = metadata.id.connectionId.sessionAlias
                val sessionGroup = metadata.id.connectionId.sessionGroup
                result.putAll(metadata.propertiesMap)
                result[AbstractTh2MsgFilterStrategy.BOOK_KEY] = metadata.id.bookName
                result[AbstractTh2MsgFilterStrategy.SESSION_GROUP_KEY] = sessionGroup.ifEmpty { sessionAlias }
                result[AbstractTh2MsgFilterStrategy.SESSION_ALIAS_KEY] = metadata.id.connectionId.sessionAlias
                result[AbstractTh2MsgFilterStrategy.DIRECTION_KEY] = metadata.id.direction.name
                result[AbstractTh2MsgFilterStrategy.PROTOCOL_KEY] = metadata.protocol
            }
            else -> throw IllegalStateException("Message has not messages: ${message.toJson()}")
        }

        return result
    }
}
/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.message

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.ListValue
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.NullValue
import com.exactpro.th2.common.grpc.Value

class ParsedMessageBuilder : MessageBuilder<Message>() {
    private var messageType: String? = null
    private var fields = mutableMapOf<String, Value>()

    override fun toProto(parentEventId: EventID): Message {
        val metadataBuilder = MessageMetadata.newBuilder()
            .setId(getMessageId())
        if (messageType != null) {
            metadataBuilder.messageType = messageType
        }
        if (properties != null) {
            metadataBuilder.putAllProperties(properties)
        }
        if (protocol != null) {
            metadataBuilder.protocol = protocol
        }
        val timestamp = getTimestamp()
        if (timestamp != null) {
            metadataBuilder.timestamp = timestamp
        }
        return Message.newBuilder()
            .setParentEventId(parentEventId)
            .setMetadata(metadataBuilder)
            .putAllFields(fields)
            .build()
    }

    fun addNullField(field: String): ParsedMessageBuilder {
        this.fields[field] = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()
        return this
    }

    fun addSimpleField(field: String, value: String): ParsedMessageBuilder {
        this.fields[field] = Value.newBuilder().setSimpleValue(value).build()
        return this
    }

    fun addMessageField(field: String, message: Message): ParsedMessageBuilder {
        this.fields[field] = Value.newBuilder().setMessageValue(message).build()
        return this
    }

    fun addListField(field: String, listValue: ListValue): ParsedMessageBuilder {
        this.fields[field] = Value.newBuilder().setListValue(listValue).build()
        return this
    }
}
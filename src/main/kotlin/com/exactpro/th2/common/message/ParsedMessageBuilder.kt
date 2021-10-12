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
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.grpc.Value
import java.time.Instant

class ParsedMessageBuilder() : MessageBuilder<Message>() {
    private lateinit var messageType: String
    private var fields = mutableMapOf<String, Value>()

    constructor(rawMessageMetadata: RawMessageMetadata) : this() {
        directionValue = rawMessageMetadata.id.directionValue
        sequence = rawMessageMetadata.id.sequence
        subsequences = rawMessageMetadata.id.subsequenceList
        timestamp = Instant.ofEpochSecond(
            rawMessageMetadata.timestamp.seconds,
            rawMessageMetadata.timestamp.nanos.toLong()
        )
        properties = rawMessageMetadata.propertiesMap
        protocol = rawMessageMetadata.protocol
    }

    override fun toProto(parentEventId: EventID): Message {
        return Message.newBuilder()
            .setParentEventId(parentEventId)
            .setMetadata(
                MessageMetadata.newBuilder().also {
                    it.id = getMessageId()
                    it.timestamp = getTimestamp()
                    it.messageType = messageType
                    it.putAllProperties(properties)
                    it.protocol = protocol
                }
            )
            .putAllFields(fields)
            .build()
    }

    fun messageType(messageType: String): ParsedMessageBuilder {
        this.messageType = messageType
        return this
    }

    fun addNullField(field: String): ParsedMessageBuilder {
        fields[field] = Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()
        return this
    }

    fun addSimpleField(field: String, value: String): ParsedMessageBuilder {
        fields[field] = Value.newBuilder().setSimpleValue(value).build()
        return this
    }

    fun addSimpleListField(field: String, values: List<String>): ParsedMessageBuilder {
        fields[field] = Value
            .newBuilder()
            .setListValue(listValueFrom(values, Value.newBuilder()::setSimpleValue))
            .build()
        return this
    }

    fun addMessageField(field: String, message: Message): ParsedMessageBuilder {
        fields[field] = Value.newBuilder().setMessageValue(message).build()
        return this
    }

    fun addMessageListField(field: String, messages: List<Message>): ParsedMessageBuilder {
        fields[field] = Value
            .newBuilder()
            .setListValue(listValueFrom(messages, Value.newBuilder()::setMessageValue))
            .build()
        return this
    }

    private fun <T> listValueFrom(values: List<T>, listValueBuilder: (T) -> Value.Builder) =
        ListValue
            .newBuilder()
            .addAllValues(
                values
                    .asSequence()
                    .map { listValueBuilder(it).build() }
                    .asIterable()
            )
}
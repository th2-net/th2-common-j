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
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessageMetadata
import java.time.Instant

class ParsedMessageBuilder() : MessageBuilder<ParsedMessageBuilder>() {
    private var messageType: String = ""
    private val innerMessageBuilder = ParsedInnerMessageBuilder()

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

    override fun builder() = this

    fun toProto(parentEventId: EventID?): Message {
        return Message.newBuilder().also { message ->
            if (parentEventId != null) {
                message.parentEventId = parentEventId
            }
            message
                .setMetadata(
                    MessageMetadata.newBuilder().also { metadata ->
                        metadata.id = getMessageId()
                        metadata.timestamp = getTimestamp()
                        metadata.messageType = messageType
                        metadata.putAllProperties(properties)
                        metadata.protocol = protocol
                    }
                )
                .putAllFields(innerMessageBuilder.toValueMap())
        }.build()
    }

    fun messageType(messageType: String): ParsedMessageBuilder {
        this.messageType = messageType
        return builder()
    }

    fun addNullField(field: String): ParsedMessageBuilder {
        innerMessageBuilder.addNullField(field)
        return builder()
    }

    fun addSimpleField(field: String, value: String): ParsedMessageBuilder {
        innerMessageBuilder.addSimpleField(field, value)
        return builder()
    }

    fun addSimpleListField(field: String, vararg values: String): ParsedMessageBuilder {
        innerMessageBuilder.addSimpleListField(field, *values)
        return builder()
    }

    fun addMessageField(field: String, builder: ParsedInnerMessageBuilder): ParsedMessageBuilder {
        innerMessageBuilder.addMessageField(field, builder)
        return builder()
    }

    fun addMessageListField(field: String, vararg builders: ParsedInnerMessageBuilder): ParsedMessageBuilder {
        innerMessageBuilder.addMessageListField(field, *builders)
        return builder()
    }
}

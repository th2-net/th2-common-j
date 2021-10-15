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
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.google.protobuf.ByteString
import java.time.Instant

class RawMessageBuilder() : MessageBuilder<RawMessageBuilder>() {
    private lateinit var bytes: ByteArray

    constructor(messageMetadata: MessageMetadata) : this() {
        directionValue = messageMetadata.id.directionValue
        sequence = messageMetadata.id.sequence
        subsequences = messageMetadata.id.subsequenceList
        timestamp = Instant.ofEpochSecond(
            messageMetadata.timestamp.seconds,
            messageMetadata.timestamp.nanos.toLong()
        )
        properties = messageMetadata.propertiesMap
        protocol = messageMetadata.protocol
    }

    override fun builder() = this

    fun toProto(parentEventId: EventID?): RawMessage {
        return RawMessage.newBuilder().apply {
            if (parentEventId != null) {
                setParentEventId(parentEventId)
            }
            setMetadata(
                RawMessageMetadata.newBuilder().also {
                    it.id = getMessageId()
                    it.timestamp = getTimestamp()
                    it.putAllProperties(properties)
                    it.protocol = protocol
                }
            )
            body = ByteString.copyFrom(bytes)
        }.build()
    }

    fun bytes(bytes: ByteArray): RawMessageBuilder {
        this.bytes = bytes
        return builder()
    }
}
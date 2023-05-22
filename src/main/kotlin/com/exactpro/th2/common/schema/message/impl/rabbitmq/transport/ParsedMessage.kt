/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.transport

import com.google.auto.value.AutoBuilder
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.util.Collections

class ParsedMessage(
    override val id: MessageId,
    override val eventId: EventId? = null,
    val type: String,
    override val metadata: Map<String, String> = emptyMap(),
    override val protocol: String = "",
    val rawBody: ByteBuf = Unpooled.buffer(),
    body: Map<String, Any?> = DEFAULT_BODY,
) : Message<Map<String, Any?>> {
    private var bodySupplier: (ByteBuf) -> MutableMap<String, Any?> = DEFAULT_BODY_SUPPLIER

    /**
     * Is set to `true` if the [body] is deserialized from the [rawBody].
     * If the [body] is set directly returns `false`
     */
    val isBodyInRaw: Boolean = body === DEFAULT_BODY

    override val body: Map<String, Any?> by lazy {
        if (body == DEFAULT_BODY) {
            bodySupplier.invoke(rawBody).apply {
                rawBody.resetReaderIndex()
            }
        } else {
            body
        }
    }


    @AutoBuilder
    abstract class Builder : Message.Builder<Builder> {
        abstract fun setType(type: String): Builder
        abstract fun setRawBody(rawBody: ByteBuf): Builder
        abstract fun setBody(body: Map<String, Any?>): Builder
        protected abstract fun autoBuild(): ParsedMessage

        @JvmOverloads
        fun build(bodySupplier: (ByteBuf) -> MutableMap<String, Any?> = DEFAULT_BODY_SUPPLIER): ParsedMessage {
            return autoBuild().apply {
                this.bodySupplier = bodySupplier
            }
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ParsedMessage

        if (id != other.id) return false
        if (eventId != other.eventId) return false
        if (type != other.type) return false
        if (metadata != other.metadata) return false
        if (protocol != other.protocol) return false
        return rawBody == other.rawBody
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + (eventId?.hashCode() ?: 0)
        result = 31 * result + type.hashCode()
        result = 31 * result + metadata.hashCode()
        result = 31 * result + protocol.hashCode()
        result = 31 * result + rawBody.hashCode()
        return result
    }

    override fun toString(): String {
        return "ParsedMessage(id=$id, " +
                "eventId=$eventId, " +
                "type='$type', " +
                "metadata=$metadata, " +
                "protocol='$protocol', " +
                "rawBody=${rawBody.toString(Charsets.UTF_8)}, " +
                "body=${
                    if (isBodyInRaw) {
                        "!checkRawBody"
                    } else {
                        body.toString()
                    }
                })"
    }


    companion object {
        /**
         * We want to be able to identify the default body by reference.
         * So, that is why we use unmodifiableMap with emptyMap
         * Otherwise, we won't be able to identify it
         */
        private val DEFAULT_BODY: Map<String, Any?> = Collections.unmodifiableMap(emptyMap())

        val DEFAULT_BODY_SUPPLIER: (ByteBuf) -> MutableMap<String, Any?> = { hashMapOf() }

        @JvmStatic
        fun builder(): ParsedMessage.Builder =
            AutoBuilder_ParsedMessage_Builder()
    }
}
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

data class ParsedMessage(
    override val id: MessageId,
    override val eventId: EventId? = null,
    val type: String,
    override val metadata: Map<String, String> = emptyMap(),
    override val protocol: String = "",
    val rawBody: ByteBuf = Unpooled.buffer(),
) : Message<MutableMap<String, Any>> {
    private var bodySupplier: (ByteBuf) -> MutableMap<String, Any> = DEFAULT_BODY_SUPPLIER
    internal val bodyChanged: Boolean
        // probably, it requires something like observable map to track changes
        // because right now if user requests body and does not change it
        // we still will have to serialize it again (even though the rawBody is not changed)
        get() = _body != null

    private var _body: MutableMap<String, Any>? = null
    /** The body is not mutable by default */
    override var body: MutableMap<String, Any>
        get() {
            if (_body == null) {
                _body = bodySupplier(rawBody)
            }
            return requireNotNull(_body) { "body is null" }
        }
        set(value) {
            _body = value
        }

    @AutoBuilder
    abstract class Builder : Message.Builder<Builder> {
        abstract fun setType(type: String): Builder
        abstract fun setRawBody(rawBody: ByteBuf): Builder
        protected abstract fun autoBuild(): ParsedMessage
        @JvmOverloads
        fun build(bodySupplier: (ByteBuf) -> MutableMap<String, Any> = DEFAULT_BODY_SUPPLIER): ParsedMessage {
            return autoBuild().apply {
                this.bodySupplier = bodySupplier
            }
        }
    }

    companion object {
        val DEFAULT_BODY_SUPPLIER: (ByteBuf) -> MutableMap<String, Any> = { hashMapOf() }
        @JvmStatic
        fun builder(): ParsedMessage.Builder =
            AutoBuilder_ParsedMessage_Builder()
    }
}
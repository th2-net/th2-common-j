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

import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.BOOK_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.DIRECTION_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.MESSAGE_TYPE_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.PROTOCOL_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.SESSION_ALIAS_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.SESSION_GROUP_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.checkFieldValue
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.INCOMING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.OUTGOING
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import mu.KotlinLogging
import com.exactpro.th2.common.grpc.Direction as ProtoDirection

fun Collection<RouterFilter>.filter(batch: GroupBatch): GroupBatch? {
    if (isEmpty()) {
        return batch
    }

    forEach { filterSet ->
        if (!filterSet.metadata[BOOK_KEY].verify(batch.book)) {
            return@forEach
        }
        if (!filterSet.metadata[SESSION_GROUP_KEY].verify(batch.sessionGroup)) {
            return@forEach
        }

        if (!filterSet.metadata[SESSION_ALIAS_KEY].verify(batch.groups) { id.sessionAlias }) {
            return@forEach
        }
        if (!filterSet.metadata[MESSAGE_TYPE_KEY].verify(batch.groups) { if (this is ParsedMessage) type else "" }) {
            return@forEach
        }
        if (!filterSet.metadata[DIRECTION_KEY].verify(batch.groups) { id.direction.proto.name }) {
            return@forEach
        }
        if (!filterSet.metadata[PROTOCOL_KEY].verify(batch.groups) { protocol }) {
            return@forEach
        }

        return batch
    }

    return null
}

private fun Collection<FieldFilterConfiguration>?.verify(value: String): Boolean {
    if (isNullOrEmpty()) {
        return true
    }
    return all { it.checkFieldValue(value) }
}

private inline fun Collection<FieldFilterConfiguration>?.verify(
    messageGroups: Collection<MessageGroup>,
    value: Message<*>.() -> String
): Boolean {
    if (isNullOrEmpty()) {
        return true
    }

    // Illegal cases when groups or messages are empty
    if (messageGroups.isEmpty()) {
        return false
    }
    val firstGroup = messageGroups.first()
    if (firstGroup.messages.isEmpty()) {
        return false
    }

    return all { filter -> filter.checkFieldValue(firstGroup.messages.first().value()) }
}

fun GroupBatch.toByteArray(): ByteArray = Unpooled.buffer().run {
    GroupBatchCodec.encode(this@toByteArray, this@run)
    ByteArray(readableBytes()).apply(::readBytes)
}

fun ByteBuf.toByteArray(): ByteArray = ByteArray(readableBytes())
    .apply(::readBytes).also { resetReaderIndex() }

val Direction.proto: ProtoDirection
    get() = when (this) {
        INCOMING -> FIRST
        OUTGOING -> SECOND
    }

val ProtoDirection.transport: Direction
    get() = when (this) {
        FIRST -> INCOMING
        SECOND -> OUTGOING
        else -> error("Unsupported $this direction in the th2 transport protocol")
    }

private const val trackedSecondaryClOrdID = "10050000"
private val trackedSecondaryClOrdIDBytes = trackedSecondaryClOrdID.toByteArray()

private fun containsSubArray(buffer: ByteBuf): Boolean {
    var i = 0
    var isFound = false
    buffer.forEachByte { byte ->
        if (byte == trackedSecondaryClOrdIDBytes[i]) {
            if (++i == trackedSecondaryClOrdIDBytes.size) {
                isFound = true
                return@forEachByte false // stop processing
            }
        } else {
            i = 0
        }
        true
    }
    return isFound
}

private val LOGGER = KotlinLogging.logger {}
fun logTrackedMessages(batch: GroupBatch, logMessage: String) {
    batch.groups.flatMap { it.messages }.forEach {
        val isLogging = when(it) {
            is RawMessage -> containsSubArray(it.body)
            is ParsedMessage -> trackedSecondaryClOrdID == it.body["SecondaryClOrdID"].toString()
            else -> false
        }

        if (isLogging) {
            LOGGER.error { "$logMessage. msg = $it" }
        }
    }
}
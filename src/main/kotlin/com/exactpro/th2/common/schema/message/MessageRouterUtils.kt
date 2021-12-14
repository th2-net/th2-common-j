/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

@file:JvmName("MessageRouterUtils")

package com.exactpro.th2.common.schema.message

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.logId
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.QueueAttribute.EVENT
import com.exactpro.th2.common.schema.message.QueueAttribute.PUBLISH
import com.google.protobuf.MessageOrBuilder
import org.apache.commons.lang3.exception.ExceptionUtils

fun MessageRouter<EventBatch>.storeEvent(
    event: Event,
    parentId: EventID
) = storeEvent(event, event.toProto(parentId))


fun MessageRouter<EventBatch>.storeEvent(
    event: Event,
    bookName: String
) = storeEvent(event, event.toProto(bookName))

private fun MessageRouter<EventBatch>.storeEvent(
    event: Event,
    protoEvent: com.exactpro.th2.common.grpc.Event
): Event = event.apply {
    sendAll(
        EventBatch.newBuilder().addEvents(protoEvent).build(),
        PUBLISH.toString(),
        EVENT.toString()
    )
}

fun MessageRouter<EventBatch>.storeEvent(
    parentId: EventID,
    name: String,
    type: String,
    cause: Throwable? = null
): Event = Event.start().apply {
    endTimestamp()
    name(name)
    type(type)
    status(if (cause != null) FAILED else PASSED)

    var error = cause

    while (error != null) {
        bodyData(EventUtils.createMessageBean(ExceptionUtils.getMessage(error)))
        error = error.cause
    }

    storeEvent(this, parentId)
}

@Deprecated(message = "Please use MessageUtils.toJson", replaceWith = ReplaceWith("toJson(true)", imports = ["com.exactpro.th2.common.message.toJson"]), level = DeprecationLevel.WARNING)
fun MessageOrBuilder.toJson() : String = toJson(true)

fun appendAttributes(
    vararg attributes: String,
    requiredAttributes: () -> Set<String>
): Set<String> {
    if (attributes.isEmpty()) {
        return requiredAttributes()
    }
    return mutableSetOf(*attributes).apply {
        addAll(requiredAttributes())
    }
}

fun MessageGroupBatch.toShortDebugString(): String = buildString {
    append("MessageGroupBatch(ids = ")

    groupsList.asSequence()
        .flatMap { it.messagesList.asSequence() }
        .map(AnyMessage::logId)
        .toSortedSet()
        .apply { append(this) }

    append(')')
}
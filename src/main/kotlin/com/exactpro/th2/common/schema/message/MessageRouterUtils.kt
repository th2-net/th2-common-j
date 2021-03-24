/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.schema.message.QueueAttribute.EVENT
import com.exactpro.th2.common.schema.message.QueueAttribute.PUBLISH
import com.google.protobuf.Message
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.util.JsonFormat

fun MessageRouter<EventBatch>.storeEvent(
    event: Event,
    parentId: String? = null
): Event = event.apply {
    val batch = EventBatch.newBuilder().addEvents(toProtoEvent(parentId)).build()
    send(batch, PUBLISH.toString(), EVENT.toString())
}

fun MessageRouter<EventBatch>.storeEvent(
    parentId: String,
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
        bodyData(EventUtils.createMessageBean(error.message))
        error = error.cause
    }

    storeEvent(this, parentId)
}

// Not using default parameters for java compatibility
@Deprecated(message = "Only for back compatibility", replaceWith = ReplaceWith("toJson(true)", imports = ["com.exactpro.th2.common.schema.message.toJson"]), level = DeprecationLevel.WARNING)
fun MessageOrBuilder.toJson() : String = this.toJson(true)

fun MessageOrBuilder.toJson(short: Boolean): String = JsonFormat.printer().includingDefaultValueFields().let {
    (if (short) it.omittingInsignificantWhitespace() else it).print(this)
}

fun <T: Message.Builder> T.fromJson(json: String) : T {
    JsonFormat.parser().ignoringUnknownFields().merge(json, this)
    return this
}
/*
 * Copyright 2020-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.monitor

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.bean.Message
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.MessageRouterMonitor
import org.slf4j.helpers.MessageFormatter.arrayFormat

class EventMessageRouterMonitor(
    private val router: MessageRouter<EventBatch>,
    private val parentEventId: EventID
) :
    MessageRouterMonitor {

    override fun onInfo(msg: String, vararg args: Any?) {
        router.send(createEventBatch("Event in message router", arrayFormat(msg, args).message, Event.Status.PASSED))
    }

    override fun onWarn(msg: String, vararg args: Any?) {
        router.send(createEventBatch("Warn message in message router", arrayFormat(msg, args).message, Event.Status.FAILED))
    }

    override fun onError(msg: String, vararg args: Any?) {
        router.send(createEventBatch("Error message in message router", arrayFormat(msg, args).message, Event.Status.FAILED))
    }

    private fun createEventBatch(name: String, msg: String, status: Event.Status) =
        EventBatch.newBuilder().also {
            it.addEvents(
                Event.start()
                    .name(name)
                    .bodyData(Message().apply { data = msg; type = "message" })
                    .status(status)
                    .type("event")
                    .toProto(parentEventId)
            )
        }.build()
}
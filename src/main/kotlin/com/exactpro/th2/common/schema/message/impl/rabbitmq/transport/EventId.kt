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

import java.time.Instant

data class EventId(
    var id: String = "",
    var book: String = "",
    var scope: String = "",
    var timestamp: Instant = Instant.EPOCH,
) : Cleanable {

    override fun clean() {
        check(this !== DEFAULT_INSTANCE) {
            "Object can be cleaned because it is default instance"
        }
        id= ""
        book = ""
        scope = ""
        timestamp = Instant.EPOCH
    }

    companion object {
        val DEFAULT_INSTANCE: EventId = EventId()  // FIXME: do smth about its mutability
        fun newMutable() = EventId()
    }
}
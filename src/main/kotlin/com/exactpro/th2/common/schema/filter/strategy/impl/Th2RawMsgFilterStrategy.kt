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
package com.exactpro.th2.common.schema.filter.strategy.impl

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.toJson
import com.google.protobuf.Message

class Th2RawMsgFilterStrategy : AbstractFilterStrategy<Message>() {
    override fun getFields(message: Message): MutableMap<String, String> {
        check(message is RawMessage) { "Message is not an ${RawMessage::class.qualifiedName}: ${message.toJson()}" }
        val metadata = message.metadata
        return mutableMapOf(
            AbstractTh2MsgFilterStrategy.SESSION_ALIAS_KEY to metadata.id.connectionId.sessionAlias,
            AbstractTh2MsgFilterStrategy.DIRECTION_KEY to metadata.id.direction.name
        )
    }
}
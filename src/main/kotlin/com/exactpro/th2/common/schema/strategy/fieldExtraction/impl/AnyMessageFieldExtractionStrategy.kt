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

package com.exactpro.th2.common.schema.strategy.fieldExtraction.impl

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.schema.message.toJson
import com.exactpro.th2.common.schema.strategy.fieldExtraction.AbstractTh2MsgFieldExtraction
import com.exactpro.th2.common.schema.strategy.fieldExtraction.FieldExtractionStrategy

class AnyMessageFieldExtractionStrategy : FieldExtractionStrategy {

    override fun getFields(message: com.google.protobuf.Message): MutableMap<String, String> {
        check(message is AnyMessage) { "Message is not an ${AnyMessage::class.qualifiedName}: ${message.toJson()}" }

        val result = HashMap<String, String>();

        if (message.hasMessage()) {
            result.putAll(message.message.fieldsMap.mapValues { it.value.simpleValue })

            val metadata = message.message.metadata
            result[AbstractTh2MsgFieldExtraction.SESSION_ALIAS_KEY] = metadata.id.connectionId.sessionAlias
            result[AbstractTh2MsgFieldExtraction.MESSAGE_TYPE_KEY] = message.message.descriptorForType.name
            result[AbstractTh2MsgFieldExtraction.DIRECTION_KEY] = metadata.id.direction.name
        } else {
            val metadata = message.rawMessage.metadata
            result[AbstractTh2MsgFieldExtraction.SESSION_ALIAS_KEY] = metadata.id.connectionId.sessionAlias
            result[AbstractTh2MsgFieldExtraction.DIRECTION_KEY] = metadata.id.direction.name
        }

        return result
    }
}
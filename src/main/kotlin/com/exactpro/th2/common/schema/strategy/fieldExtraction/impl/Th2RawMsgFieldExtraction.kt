package com.exactpro.th2.common.schema.strategy.fieldExtraction.impl

import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.schema.message.toJson
import com.exactpro.th2.common.schema.strategy.fieldExtraction.AbstractTh2MsgFieldExtraction
import com.exactpro.th2.common.schema.strategy.fieldExtraction.FieldExtractionStrategy
import com.google.protobuf.Message

class Th2RawMsgFieldExtraction : FieldExtractionStrategy {
    override fun getFields(message: Message): MutableMap<String, String> {
        check(message is RawMessage) { "Message is not an ${RawMessage::class.qualifiedName}: ${message.toJson()}" }
        val metadata = message.metadata
        return mutableMapOf(
            AbstractTh2MsgFieldExtraction.SESSION_ALIAS_KEY to metadata.id.connectionId.sessionAlias,
            AbstractTh2MsgFieldExtraction.DIRECTION_KEY to metadata.id.direction.name
        )
    }
}
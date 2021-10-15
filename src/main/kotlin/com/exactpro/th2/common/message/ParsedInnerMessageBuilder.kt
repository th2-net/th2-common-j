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

package com.exactpro.th2.common.message

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.value.nullValue
import com.exactpro.th2.common.value.toListValue
import com.exactpro.th2.common.value.toValue

class ParsedInnerMessageBuilder {
    private var fields = mutableMapOf<String, Field>()

    fun addNullField(field: String): ParsedInnerMessageBuilder {
        fields[field] = NullField()
        return this
    }

    fun addSimpleField(field: String, value: String): ParsedInnerMessageBuilder {
        fields[field] = SimpleField(value)
        return this
    }

    fun addSimpleListField(field: String, vararg values: String): ParsedInnerMessageBuilder {
        fields[field] = SimpleListField(*values)
        return this
    }

    fun addMessageField(field: String, builder: ParsedInnerMessageBuilder): ParsedInnerMessageBuilder {
        fields[field] = MessageField(builder)
        return this
    }

    fun addMessageListField(field: String, vararg builders: ParsedInnerMessageBuilder): ParsedInnerMessageBuilder {
        fields[field] = MessageListField(*builders)
        return this
    }

    fun toValueMap() = fields.asSequence().map { it.key to it.value.toValue() }.toMap()

    private fun toProto(): Message = Message
        .newBuilder()
        .also { it.putAllFields(toValueMap()) }
        .build()

    companion object {
        private interface Field {
            fun toValue(): Value
        }

        private class NullField : Field {
            override fun toValue() = nullValue()
        }

        private class SimpleField(private val value: String) : Field {
            override fun toValue() = value.toValue()
        }

        private class SimpleListField(private vararg val values: String) : Field {
            override fun toValue(): Value = values.toListValue().toValue()
        }

        private class MessageField(private val builder: ParsedInnerMessageBuilder) : Field {
            override fun toValue() = builder.toProto().toValue()
        }

        private class MessageListField(private vararg val builders: ParsedInnerMessageBuilder) : Field {
            override fun toValue(): Value = builders.map { it.toProto().toValue() }.toListValue().toValue()
        }
    }
}
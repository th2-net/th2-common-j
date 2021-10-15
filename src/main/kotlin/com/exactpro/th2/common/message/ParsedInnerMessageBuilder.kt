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
    private var fields = mutableMapOf<String, Value>()

    fun addNullField(field: String): ParsedInnerMessageBuilder {
        fields[field] = nullValue()
        return this
    }

    fun addSimpleField(field: String, value: String): ParsedInnerMessageBuilder {
        fields[field] = value.toValue()
        return this
    }

    fun addSimpleListField(field: String, vararg values: String): ParsedInnerMessageBuilder {
        fields[field] = values.toListValue().toValue()
        return this
    }

    fun addMessageField(field: String, builder: ParsedInnerMessageBuilder): ParsedInnerMessageBuilder {
        fields[field] = builder.toProto().toValue()
        return this
    }

    fun addMessageListField(field: String, vararg builders: ParsedInnerMessageBuilder): ParsedInnerMessageBuilder {
        fields[field] = builders.map { it.toProto().toValue() }.toListValue().toValue()
        return this
    }

    internal fun toProto(): Message = Message.newBuilder().also { it.putAllFields(fields) }.build()
}
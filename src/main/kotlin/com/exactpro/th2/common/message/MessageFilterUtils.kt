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

package com.exactpro.th2.common.message

import com.exactpro.th2.common.value.emptyValueFilter
import com.exactpro.th2.common.value.toValueFilter
import com.exactpro.th2.infra.grpc.MessageFilter
import com.exactpro.th2.infra.grpc.ValueFilter

fun messageFilter(messageType: String): MessageFilter.Builder = MessageFilter.newBuilder().setMessageType(messageType)
fun messageFilter(): MessageFilter.Builder = MessageFilter.newBuilder()

fun MessageFilter.getField(key: String): ValueFilter? = getFieldsOrDefault(key, null)
fun MessageFilter.Builder.getField(key: String): ValueFilter? = getFieldsOrDefault(key, null)

fun MessageFilter.Builder.addField(key: String, value: Any?): MessageFilter.Builder = apply { putFields(key, value?.toValueFilter() ?: emptyValueFilter()) }

/**
 * Accepts vararg with even size. It split to pair: the first value is used as key, the second value is used as value
 */
fun MessageFilter.Builder.addFields(vararg fields: Any?): MessageFilter.Builder = apply {
    for (i in fields.indices step 2) {
        addField(fields[i] as String, fields[i + 1])
    }
}

fun MessageFilter.Builder.addFields(fields: Map<String, Any?>?): MessageFilter.Builder = apply { fields?.forEach { addField(it.key, it.value) } }

fun MessageFilter.Builder.copyField(message: MessageFilter.Builder, key: String): MessageFilter.Builder = apply { putFields(key, message.getField(key) ?: emptyValueFilter()) }
fun MessageFilter.Builder.copyField(message: MessageFilter.Builder, vararg key: String): MessageFilter.Builder = apply { key.forEach { putFields(it, message.getField(it) ?: emptyValueFilter()) } }
fun MessageFilter.Builder.copyField(message: MessageFilter, vararg key: String): MessageFilter.Builder = apply { key.forEach { putFields(it, message.getField(it) ?: emptyValueFilter()) } }

fun MessageFilter.copy(): MessageFilter.Builder = MessageFilter.newBuilder().putAllFields(fieldsMap)

fun MessageFilter.Builder.copy(): MessageFilter.Builder = MessageFilter.newBuilder().putAllFields(fieldsMap)
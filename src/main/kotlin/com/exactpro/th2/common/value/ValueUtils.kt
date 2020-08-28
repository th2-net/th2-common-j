/*******************************************************************************
 *  Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.common.value

import com.exactpro.th2.infra.grpc.ListValue
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.NullValue.NULL_VALUE
import com.exactpro.th2.infra.grpc.Value
import com.exactpro.th2.infra.grpc.Value.KindCase.LIST_VALUE
import com.exactpro.th2.infra.grpc.Value.KindCase.MESSAGE_VALUE
import com.exactpro.th2.infra.grpc.Value.KindCase.SIMPLE_VALUE
import java.math.BigDecimal

fun nullValue() = Value.newBuilder().setNullValue(NULL_VALUE).build()

fun Value.getString(): String? = if (this.kindCase == SIMPLE_VALUE) this.simpleValue else null
fun Value.getInt(): Int? = this.getString()?.toIntOrNull()
fun Value.getLong(): Long? = this.getString()?.toLongOrNull()
fun Value.getDouble(): Double? = this.getString()?.toDoubleOrNull()
fun Value.getBigDecimal(): BigDecimal? = this.getString()?.toBigDecimalOrNull()
fun Value.getMessage(): Message? = if (this.kindCase == MESSAGE_VALUE) this.messageValue else null
fun Value.getList(): ListValue? = if (this.kindCase == LIST_VALUE) this.listValue else null

fun String.toValue(): Value = Value.newBuilder().setSimpleValue(this).build()

fun Message.toValue(): Value = Value.newBuilder().setMessageValue(this).build()

fun Message.Builder.toValue(): Value = Value.newBuilder().setMessageValue(this).build()

fun Any.toValue(): Value = when (this) {
    is Message -> toValue()
    is Message.Builder -> toValue()
    is Iterable<*> -> toValue()
    else -> toString().toValue()
}

fun Iterable<*>.toValue(): Value = Value.newBuilder()
    .setListValue(
        ListValue
            .newBuilder()
            .also { builder ->
                forEach {
                    if (it != null) {
                        builder.addValues(it.toValue())
                    }
                }
            }
            .build())
    .build()

/*******************************************************************************
 *  Copyright 2020 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.infra.grpc.FilterOperation.EMPTY
import com.exactpro.th2.infra.grpc.FilterOperation.NOT_EMPTY
import com.exactpro.th2.infra.grpc.ListValueFilter
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageFilter
import com.exactpro.th2.infra.grpc.ValueFilter
import com.exactpro.th2.infra.grpc.ValueFilter.KindCase.LIST_FILTER
import com.exactpro.th2.infra.grpc.ValueFilter.KindCase.MESSAGE_FILTER
import com.exactpro.th2.infra.grpc.ValueFilter.KindCase.SIMPLE_FILTER
import java.math.BigDecimal

fun emptyValueFilter() = ValueFilter.newBuilder().setOperation(EMPTY).build()
fun notEmptyValueFilter() = ValueFilter.newBuilder().setOperation(NOT_EMPTY).build()

fun ValueFilter.getString(): String? = if (kindCase == SIMPLE_FILTER) simpleFilter else null
fun ValueFilter.getInt(): Int? = getString()?.toIntOrNull()
fun ValueFilter.getLong(): Long? = getString()?.toLongOrNull()
fun ValueFilter.getDouble(): Double? = getString()?.toDoubleOrNull()
fun ValueFilter.getBigDecimal(): BigDecimal? = getString()?.toBigDecimalOrNull()
fun ValueFilter.getMessage(): MessageFilter? = if (kindCase == MESSAGE_FILTER) messageFilter else null
fun ValueFilter.getList(): ListValueFilter? = if (kindCase == LIST_FILTER) listFilter else null

fun String.toValueFilter(): ValueFilter = ValueFilter.newBuilder().setSimpleFilter(this).build()

fun MessageFilter.toValueFilter(): ValueFilter = ValueFilter.newBuilder().setMessageFilter(this).build()

fun MessageFilter.Builder.toValueFilter(): ValueFilter = ValueFilter.newBuilder().setMessageFilter(this).build()

fun Any.toValueFilter(): ValueFilter = when (this) {
    is MessageFilter -> toValueFilter()
    is MessageFilter.Builder -> toValueFilter()
    is Iterable<*> -> toValueFilter()
    else ->  toString().toValueFilter()
}

fun Iterable<*>.toValueFilter(): ValueFilter = ValueFilter.newBuilder()
    .setListFilter(
        ListValueFilter.newBuilder()
            .also { builder ->
                forEach {
                    if (it != null) {
                        builder.addValues(it.toValueFilter())
                    }
                }
            }
            .build())
    .build()

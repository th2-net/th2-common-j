/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.common.value

import com.exactpro.th2.common.grpc.FilterOperation.EMPTY
import com.exactpro.th2.common.grpc.FilterOperation.EQUAL
import com.exactpro.th2.common.grpc.FilterOperation.NOT_EMPTY
import com.exactpro.th2.common.grpc.FilterOperation.NOT_EQUAL
import com.exactpro.th2.common.grpc.ListValueFilter
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.grpc.ValueFilter.KindCase.MESSAGE_FILTER
import com.exactpro.th2.common.grpc.ValueFilter.KindCase.SIMPLE_FILTER
import java.math.BigDecimal
import java.util.stream.Stream

fun equalValueFilter() = ValueFilter.newBuilder().setOperation(EQUAL).build()
fun notEqualValueFilter() = ValueFilter.newBuilder().setOperation(NOT_EQUAL).build()
fun emptyValueFilter() = ValueFilter.newBuilder().setOperation(EMPTY).build()
fun notEmptyValueFilter() = ValueFilter.newBuilder().setOperation(NOT_EMPTY).build()

fun ValueFilter.getString(): String? = if (kindCase == SIMPLE_FILTER) simpleFilter else null
fun ValueFilter.getInt(): Int? = getString()?.toIntOrNull()
fun ValueFilter.getLong(): Long? = getString()?.toLongOrNull()
fun ValueFilter.getDouble(): Double? = getString()?.toDoubleOrNull()
fun ValueFilter.getBigDecimal(): BigDecimal? = getString()?.toBigDecimalOrNull()
fun ValueFilter.getMessage(): MessageFilter? = if (kindCase == MESSAGE_FILTER) messageFilter else null

fun String.toValueFilter(): ValueFilter = ValueFilter.newBuilder().setSimpleFilter(this).build()

fun MessageFilter.toValueFilter(): ValueFilter = ValueFilter.newBuilder().setMessageFilter(this).build()

fun MessageFilter.Builder.toValueFilter(): ValueFilter = ValueFilter.newBuilder().setMessageFilter(this).build()

fun ListValueFilter.toValueFilter() : ValueFilter = ValueFilter.newBuilder().setListFilter(this).build()

fun ListValueFilter.Builder.toValueFilter() : ValueFilter = ValueFilter.newBuilder().setListFilter(this).build()

fun Any.toValueFilter(): ValueFilter = when (this) {
    is MessageFilter -> toValueFilter()
    is MessageFilter.Builder -> toValueFilter()
    is ListValueFilter -> toValueFilter()
    is ListValueFilter.Builder -> toValueFilter()
    is Iterator<*> -> toValueFilter()
    is Iterable<*> -> toValueFilter()
    is Sequence<*> -> iterator().toValueFilter()
    is Stream<*> -> iterator().toValueFilter()
    is Array<*> -> toValueFilter()
    is BooleanArray -> toValueFilter()
    is ByteArray -> toValueFilter()
    is CharArray -> toValueFilter()
    is ShortArray -> toValueFilter()
    is IntArray -> toValueFilter()
    is LongArray -> toValueFilter()
    is FloatArray -> toValueFilter()
    is DoubleArray -> toValueFilter()
    else -> toString().toValueFilter()
}

fun Iterator<*>.toValueFilter(): ValueFilter = ListValueFilter.newBuilder()
            .also { builder ->
        while (hasNext()) {
            next()?.also {
                        builder.addValues(it.toValueFilter())
                    }
                }
    }.toValueFilter()

fun Iterable<*>.toValueFilter(): ValueFilter = iterator().toValueFilter()

fun Array<*>.toValueFilter(): ValueFilter = iterator().toValueFilter()

fun BooleanArray.toValueFilter(): ValueFilter = toTypedArray().toValueFilter()
fun ByteArray.toValueFilter(): ValueFilter = toTypedArray().toValueFilter()
fun CharArray.toValueFilter(): ValueFilter = toTypedArray().toValueFilter()
fun ShortArray.toValueFilter(): ValueFilter = toTypedArray().toValueFilter()
fun IntArray.toValueFilter(): ValueFilter = toTypedArray().toValueFilter()
fun LongArray.toValueFilter(): ValueFilter = toTypedArray().toValueFilter()
fun FloatArray.toValueFilter(): ValueFilter = toTypedArray().toValueFilter()
fun DoubleArray.toValueFilter(): ValueFilter = toTypedArray().toValueFilter()

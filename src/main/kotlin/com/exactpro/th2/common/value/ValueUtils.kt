/*****************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

@file:JvmName("ValueUtils")

package com.exactpro.th2.common.value

import com.exactpro.th2.common.grpc.ListValue
import com.exactpro.th2.common.grpc.ListValueOrBuilder
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageOrBuilder
import com.exactpro.th2.common.grpc.NullValue.NULL_VALUE
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.grpc.Value.KindCase.SIMPLE_VALUE
import com.exactpro.th2.common.grpc.ValueOrBuilder
import java.math.BigDecimal
import java.math.BigInteger

fun nullValue(): Value = Value.newBuilder().setNullValue(NULL_VALUE).build()
fun listValue() : ListValue.Builder = ListValue.newBuilder()

fun Value.getString(): String? = takeIf { kindCase == SIMPLE_VALUE }?.simpleValue
fun Value.getInt(): Int? = this.getString()?.toIntOrNull()
fun Value.getLong(): Long? = this.getString()?.toLongOrNull()
fun Value.getDouble(): Double? = this.getString()?.toDoubleOrNull()
fun Value.getBigInteger(): BigInteger? = this.getString()?.toBigIntegerOrNull()
fun Value.getBigDecimal(): BigDecimal? = this.getString()?.toBigDecimalOrNull()
fun Value.getMessage(): Message? = takeIf(Value::hasMessageValue)?.messageValue
fun Value.getList() : List<Value>? = takeIf(Value::hasListValue)?.listValue?.valuesList

fun Value.Builder.updateList(updateFunc: ListValue.Builder.() -> ListValueOrBuilder) : Value.Builder = apply { check(hasListValue()) { "Can not find list value" }; updateOrAddList(updateFunc) }
fun Value.Builder.updateString(updateFunc: String.() -> String) : ValueOrBuilder = apply { simpleValue = updateFunc(simpleValue ?: throw NullPointerException("Can not find simple value")) }
fun Value.Builder.updateMessage(updateFunc: Message.Builder.() -> MessageOrBuilder) : Value.Builder = apply { check(hasMessageValue()) { "Can not find message value" }; updateOrAddMessage(updateFunc) }

fun Value.Builder.updateOrAddList(updateFunc: ListValue.Builder.() -> ListValueOrBuilder) : Value.Builder = apply { updateFunc(listValueBuilder).also {
    when (it) {
        is ListValue -> listValue = it
        is ListValue.Builder -> setListValue(it)
        else -> error("Can not set list value. Wrong type = ${it::class.java.canonicalName}")
    }
} }
fun Value.Builder.updateOrAddMessage(updateFunc: Message.Builder.() -> MessageOrBuilder) : Value.Builder = apply { updateFunc(messageValueBuilder).also {
    when(it) {
        is Message -> messageValue = it
        is Message.Builder -> setMessageValue(it)
        else -> error("Can not set message value. Wrong type = ${it::class.java.canonicalName}")
    }
} }
fun Value.Builder.updateOrAddString(updateFunc: String.() -> String) : Value.Builder = apply { updateFunc(simpleValue).also { simpleValue = it } }

fun ListValue.Builder.update(i: Int, updateFunc: Value.Builder.() -> ValueOrBuilder?): ListValue.Builder = apply { updateFunc(getValuesBuilder(i))?.let { setValues(i, it.toValue()) } }
fun ListValue.Builder.updateList(i: Int, updateFunc: ListValue.Builder.() -> ListValueOrBuilder) : ListValue.Builder = apply { getValuesBuilder(i).updateList(updateFunc)}
fun ListValue.Builder.updateString(i: Int, updateFunc: String.() -> String) : ListValue.Builder = apply { getValuesBuilder(i).updateString(updateFunc) }
fun ListValue.Builder.updateMessage(i: Int, updateFunc: Message.Builder.() -> MessageOrBuilder) : ListValue.Builder = apply { getValuesBuilder(i).updateMessage(updateFunc) }

fun ListValue.Builder.updateOrAdd(i: Int, updateFunc: (Value.Builder?) -> ValueOrBuilder?) : ListValue.Builder = apply {
    updateFunc(if (i < valuesCount) getValuesBuilder(i) else null).also {
        while (i < valuesCount) {
            addValues(nullValue())
        }
        add(i, it)
    }
}
fun ListValue.Builder.updateOrAddList(i: Int, updateFunc: (ListValue.Builder?) -> ListValueOrBuilder) : ListValue.Builder = apply { updateOrAdd(i) { it?.updateOrAddList(updateFunc) ?: updateFunc(null)?.toValue() }}
fun ListValue.Builder.updateOrAddString(i: Int, updateFunc: (String?) -> String) : ListValue.Builder = apply { updateOrAdd(i) { it?.updateOrAddString(updateFunc) ?: updateFunc(null)?.toValue() } }
fun ListValue.Builder.updateOrAddMessage(i: Int, updateFunc: (Message.Builder?) -> MessageOrBuilder) : ListValue.Builder = apply { updateOrAdd(i) { it?.updateOrAddMessage(updateFunc) ?: updateFunc(null)?.toValue() }}

operator fun ListValueOrBuilder.get(i: Int) : Value = getValues(i)
operator fun ListValue.Builder.set(i: Int, value: Any?): ListValue.Builder = setValues(i, value?.toValue() ?: nullValue())

fun ListValue.Builder.add(value: Any?) : ListValue.Builder = apply { addValues(value?.toValue() ?: nullValue()) }
fun ListValue.Builder.add(i: Int, value: Any?) : ListValue.Builder = apply { addValues(i, value?.toValue() ?: nullValue()) }

fun Any.toValue(): Value = when (this) {
    is Message -> toValue()
    is Message.Builder -> toValue()
    is ListValue -> toValue()
    is ListValue.Builder -> toValue()
    is Iterator<*> -> toValue()
    is Iterable<*> -> toValue()
    is Array<*> -> toValue()
    is BooleanArray -> toValue()
    is ByteArray -> toValue()
    is CharArray -> toValue()
    is ShortArray -> toValue()
    is IntArray -> toValue()
    is LongArray -> toValue()
    is FloatArray -> toValue()
    is DoubleArray -> toValue()
    is Value -> this
    is Value.Builder -> toValue()
    else -> toString().toValue()
}

fun String.toValue(): Value = Value.newBuilder().setSimpleValue(this).build()

fun Message.toValue(): Value = Value.newBuilder().setMessageValue(this).build()
fun Message.Builder.toValue(): Value = Value.newBuilder().setMessageValue(this).build()

fun ListValue.toValue() : Value = Value.newBuilder().setListValue(this).build()
fun ListValue.Builder.toValue() : Value = Value.newBuilder().setListValue(this).build()

fun Value.Builder.toValue() : Value = this.build()

fun Iterator<*>.toValue(): Value = toListValue().toValue()
fun Iterable<*>.toValue(): Value = iterator().toValue()
fun Array<*>.toValue(): Value = iterator().toValue()

fun BooleanArray.toValue(): Value = toTypedArray().toValue()
fun ByteArray.toValue(): Value = toTypedArray().toValue()
fun CharArray.toValue(): Value = toTypedArray().toValue()
fun ShortArray.toValue(): Value = toTypedArray().toValue()
fun IntArray.toValue(): Value = toTypedArray().toValue()
fun LongArray.toValue(): Value = toTypedArray().toValue()
fun FloatArray.toValue(): Value = toTypedArray().toValue()
fun DoubleArray.toValue(): Value = toTypedArray().toValue()


fun Iterator<*>.toListValue() : ListValue = listValue().also { list ->
    forEach {
        it?.toValue().run(list::addValues)
    }
}.build()

fun Iterable<*>.toListValue() : ListValue = iterator().toListValue()
fun Array<*>.toListValue() : ListValue = iterator().toListValue()

fun BooleanArray.toListValue(): ListValue = toTypedArray().toListValue()
fun ByteArray.toListValue(): ListValue = toTypedArray().toListValue()
fun CharArray.toListValue(): ListValue = toTypedArray().toListValue()
fun ShortArray.toListValue(): ListValue = toTypedArray().toListValue()
fun IntArray.toListValue(): ListValue = toTypedArray().toListValue()
fun LongArray.toListValue(): ListValue = toTypedArray().toListValue()
fun FloatArray.toListValue(): ListValue = toTypedArray().toListValue()
fun DoubleArray.toListValue(): ListValue = toTypedArray().toListValue()

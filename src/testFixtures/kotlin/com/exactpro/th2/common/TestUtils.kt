/*
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

package com.exactpro.th2.common

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessageOrBuilder
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageBatchOrBuilder
import com.exactpro.th2.common.grpc.MessageGroupBatchOrBuilder
import com.exactpro.th2.common.grpc.MessageGroupOrBuilder
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.get
import com.exactpro.th2.common.message.getBigDecimal
import com.exactpro.th2.common.message.getDouble
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.message.getInt
import com.exactpro.th2.common.message.getList
import com.exactpro.th2.common.message.getLong
import com.exactpro.th2.common.message.getMessage
import com.exactpro.th2.common.message.getString
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.toJson
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.fail
import org.junit.platform.commons.util.StringUtils
import org.opentest4j.AssertionFailedError
import java.math.BigDecimal

fun assertEqualBatches(expected: MessageBatchOrBuilder, actual: MessageBatchOrBuilder, lazyMessage: () -> String? = {null}) {
    Assertions.assertEquals(expected.messagesCount, actual.messagesCount) {"wrong count of messages in batch: \n${actual.toJson()}"}
    expected.messagesList.forEachIndexed { i, message ->
        try {
            assertEqualMessages(message, actual.messagesList[i], lazyMessage)
        } catch (e: AssertionFailedError) {
            throw e.rewrap("Error in message from batch with index '$i'")
        }
    }
}

fun assertEqualGroups(expected: MessageGroupOrBuilder, actual: MessageGroupOrBuilder, lazyMessage: () -> String? = {null}) {
    Assertions.assertEquals(expected.messagesCount, actual.messagesCount) {"wrong count of messages in group: \n${actual.toJson()}"}
    expected.messagesList.forEachIndexed { i, message ->
        try {
            assertEqualMessages(message, actual.messagesList[i], lazyMessage)
        } catch (e: AssertionFailedError) {
            throw e.rewrap("Error in message from group with index '$i'")
        }
    }
}

fun assertEqualGroupBatches(expected: MessageGroupBatchOrBuilder, actual: MessageGroupBatchOrBuilder, lazyMessage: () -> String? = {null}) {
    Assertions.assertEquals(expected.groupsCount, actual.groupsCount) {"wrong count of groups in batch: \n${actual.toJson()}"}
    expected.groupsList.forEachIndexed { i, group ->
        try {
            assertEqualGroups(group, actual.getGroups(i), lazyMessage)
        } catch (e: AssertionFailedError) {
            throw e.rewrap("Error in group from batch with index '$i'")
        }

    }
}

fun assertEqualMessages(expected: AnyMessageOrBuilder, actual: AnyMessageOrBuilder, lazyMessage: () -> String? = {null}) {
    Assertions.assertEquals(expected.kindCase, actual.kindCase) { "Wrong message kind" }
    val ts = Timestamp.getDefaultInstance()

    val assertExpected = when (expected.kindCase) {
        AnyMessage.KindCase.MESSAGE -> expected.message.withTimestamp(ts)
        AnyMessage.KindCase.RAW_MESSAGE -> expected.rawMessage.withTimestamp(ts)
        else ->  expected
    }

    val assertActual = when (actual.kindCase) {
        AnyMessage.KindCase.MESSAGE -> actual.message.withTimestamp(ts)
        AnyMessage.KindCase.RAW_MESSAGE -> actual.rawMessage.withTimestamp(ts)
        else ->  actual
    }

    try {
        Assertions.assertEquals(assertExpected, assertActual, lazyMessage)
    } catch (e: AssertionFailedError) {
        throw e.rewrap("Error in '${actual.kindCase}'")
    }
}

fun assertEqualMessages(expected: RawMessage, actual: RawMessage, lazyMessage: () -> String? = {null}) = assertEqualMessages(AnyMessage.newBuilder().setRawMessage(expected), AnyMessage.newBuilder().setRawMessage(actual), lazyMessage)

fun assertEqualMessages(expected: Message, actual: Message, lazyMessage: () -> String? = {null}) = assertEqualMessages(AnyMessage.newBuilder().setMessage(expected), AnyMessage.newBuilder().setMessage(actual), lazyMessage)

fun buildPrefix(message: String?): String {
    return if (StringUtils.isNotBlank(message)) "$message ==> " else ""
}

fun Message.assertContains(vararg name: String) {
    name.forEach { fieldName ->
        if (!this.containsFields(fieldName)) {
            fail { "$messageType:$fieldName expected: not <null>" }
        }
    }
}

fun Message.assertNotContains(vararg name: String) {
    name.forEach { fieldName ->
        if (this.containsFields(fieldName)) {
            fail { "$messageType:$fieldName expected: <null>" }
        }
    }
}

fun Message.assertField(name: String): Value {
    this.assertContains(name)
    return this.getField(name)!!
}

fun Message.assertMessage(name: String): Message {
    this.assertContains(name)
    return this.getMessage(name)!!
}

fun Message.assertInt(name: String, expected: Int? = null): Int {
    this.assertContains(name)
    val actual = this.getInt(name)!!
    expected?.let {
        Assertions.assertEquals(expected, actual) {"Unexpected $name field value"}
    }
    return actual
}

fun Message.assertList(name: String, expected: List<Value> ? = null): List<Value> {
    this.assertContains(name)
    val actual = this.getList(name)!!
    expected?.let {
        Assertions.assertEquals(expected, actual)  {"Unexpected $name field value"}
    }
    return actual
}

fun Message.assertList(name: String, block: Value.() -> Unit) {
    this.assertContains(name)
    val actual = this.getList(name)!!
    actual.forEach(block)
}

fun Message.assertString(name: String, expected: String? = null): String {
    this.assertContains(name)
    val actual = this.getString(name)!!
    expected?.let {
        Assertions.assertEquals(expected, actual) {"Unexpected $name field value"}
    }
    return actual
}

fun Message.assertDouble(name: String, expected: Double? = null): Double {
    this.assertContains(name)
    val actual = this.getDouble(name)!!
    expected?.let {
        Assertions.assertEquals(expected, actual) {"Unexpected $name field value"}
    }
    return actual
}

@Suppress("UNCHECKED_CAST")
fun <T> Message.assertValue(name: String, expected: T? = null): T {
    this.assertContains(name)
    val actual = when (expected) {
        is Int -> this.getInt(name)
        is Double -> this.getDouble(name)
        is Long -> this.getLong(name)
        is BigDecimal -> this.getBigDecimal(name)
        is List<*> -> this.getList(name)
        is String -> this.getString(name)
        null -> this[name]
        else -> error("Cannot assert $name field value. Expected value type is not supported: ${expected.let { it::class.simpleName }}")
    }!!
    expected?.let {
        Assertions.assertEquals(expected, actual) {"Unexpected $name field value"}
    } ?: Assertions.assertNull(actual) {"Unexpected $name field value"}
    return actual as T
}

private fun Message.withTimestamp(ts: Timestamp) = toBuilder().apply {
    metadataBuilder.idBuilder.timestamp = ts
}.build()!!

private fun RawMessage.withTimestamp(ts: Timestamp) = toBuilder().apply {
    metadataBuilder.idBuilder.timestamp = ts
}.build()!!

private fun AssertionFailedError.rewrap(additional: String) = AssertionFailedError(
    "$additional.\n${message}",
    this.expected,
    this.actual,
    this.cause
)

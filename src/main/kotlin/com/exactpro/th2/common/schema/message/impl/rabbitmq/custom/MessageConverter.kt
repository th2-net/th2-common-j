/*
 * Copyright 2020-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.custom

/**
 * Converter for delivery to [T] object and from [T] to delivery.
 * The delivery is received/sent from/to message queue.
 */
interface MessageConverter<T : Any> {
    fun toByteArray(value: T): ByteArray

    fun fromByteArray(data: ByteArray): T

    /**
     * Representation for [value]. Is used to log the value when it is sent/received.
     */
    fun toTraceString(value: T): String

    /**
     * Short representation for [value]. Is used to log the value when it is sent/received.
     */
    fun toDebugString(value: T): String

    /**
     * Extracts the count of logical parts from the value. It should be 1 or more
     */
    fun extractCount(value: T): Int

    /**
     * Extracts the labels from the value.
     */
    fun getLabels(value: T) : Array<String> = emptyArray()

    companion object {
        // FIXME: when migrate to Kotlin 1.4 should be only one method with a default value for `countFrom` and @JvmOverloads annotation
        // Currently, Kotlin compiler has a bug that produces the method with illegal modifier: https://youtrack.jetbrains.com/issue/KT-35716
        @JvmStatic
        @Deprecated(
            message = "Use the method with separated toDebug and toTrace suppliers",
            replaceWith = ReplaceWith(
                """create(toBytes,fromBytes,toDebugString,toDebugString)""",
                "com.exactpro.th2.common.schema.message.impl.rabbitmq.custom.MessageConverter.Companion.create"),
            level = DeprecationLevel.WARNING
        )
        fun <T : Any> create(
            toBytes: (T) -> ByteArray,
            fromBytes: (ByteArray) -> T,
            toDebugString: (T) -> String
        ): MessageConverter<T> = create(toBytes, fromBytes, toDebugString, toDebugString)

        @JvmStatic
        fun <T : Any> create(
            toBytes: (T) -> ByteArray,
            fromBytes: (ByteArray) -> T,
            toTraceString: (T) -> String,
            toDebugString: (T) -> String
        ): MessageConverter<T> = create(toBytes, fromBytes, toTraceString, toDebugString) { 1 }

        @JvmStatic
        fun <T : Any> create(
            toBytes: (T) -> ByteArray,
            fromBytes: (ByteArray) -> T,
            toTraceString: (T) -> String,
            toDebugString: (T) -> String,
            countFrom: (T) -> Int
        ): MessageConverter<T> = create(toBytes, fromBytes, toTraceString, toDebugString, countFrom) { emptyArray() }

        @JvmStatic
        fun <T : Any> create(
            toBytes: (T) -> ByteArray,
            fromBytes: (ByteArray) -> T,
            toTraceString: (T) -> String,
            toDebugString: (T) -> String,
            countFrom: (T) -> Int,
            extractLabels: (T) -> Array<String>
        ): MessageConverter<T> = MessageConverterLambdaDelegate(toBytes, fromBytes, toTraceString, toDebugString, countFrom, extractLabels)
    }
}

private class MessageConverterLambdaDelegate<T : Any>(
    private val toBytes: (T) -> ByteArray,
    private val fromBytes: (ByteArray) -> T,
    private val toTraceString: (T) -> String,
    private val toDebugString: (T) -> String,
    private val countFrom: (T) -> Int,
    private val extractLabels: (T) -> Array<String>
) : MessageConverter<T> {
    override fun toByteArray(value: T): ByteArray = toBytes(value)

    override fun fromByteArray(data: ByteArray): T = fromBytes(data)

    override fun toTraceString(value: T): String = toTraceString.invoke(value)
    override fun toDebugString(value: T): String = toDebugString.invoke(value)
    override fun extractCount(value: T): Int = countFrom(value)
    override fun getLabels(value: T): Array<String> = extractLabels(value)
}
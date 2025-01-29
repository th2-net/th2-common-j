/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.grpc.router

import com.google.protobuf.Message
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.MethodDescriptor
import io.prometheus.client.Counter

abstract class AbstractGrpcInterceptor (
    protected val pinName: String,
) {
    companion object {
        private val LOGGER = KotlinLogging.logger {}

        fun MethodDescriptor<*, *>.toMethodDetails(pinName: String): MethodDetails {
            val idx = fullMethodName.lastIndexOf('/').coerceAtLeast(0)
            return MethodDetails(
                pinName,
                fullMethodName.substring(0, idx),
                fullMethodName.substring(idx + 1))
        }

        fun Any.publishMetrics(
            text: String,
            sizeBytesCounter: Counter.Child?,
            methodInvokeCounter: Counter.Child,
        ) {
            require(this is Message) {
                "Passed object of ${this::class.java} class is not implement the ${Message::class.java} interface"
            }

            val size: Int? = sizeBytesCounter?.let {
                serializedSize.also {
                    sizeBytesCounter.inc(it.toDouble())
                }
            }

            LOGGER.debug { "$text. ${if (size != null) "Message size = $size" else ""}" }
            methodInvokeCounter.inc()
        }
    }
}
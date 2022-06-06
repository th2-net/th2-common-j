/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.value.toValue
import io.grpc.ForwardingServerCall
import io.grpc.ForwardingServerCallListener
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.prometheus.client.Counter
import mu.KotlinLogging

class GrpcServerInterceptor(
    private val pinName: String,
    private val invokeCounter: Counter,
    private val requestBytes: Counter,
    private val responseBytes: Counter
) : ServerInterceptor {

    override fun <ReqT : Any, RespT : Any> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {

        val serviceName = call.methodDescriptor.serviceName
        val methodName = call.methodDescriptor.fullMethodName

        val forwardingCall = object : ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
            override fun sendMessage(message: RespT) {
                val (type, size) = getTypeAndSize(message)
                LOGGER.debug { "gRPC $serviceName/$methodName response message: type = $type, size = $size" }
                responseBytes.labels(pinName, serviceName, methodName).inc(size.toDouble())
                super.sendMessage(message)
            }
        }

        val listener = next.startCall(forwardingCall, headers)

        return object : ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            override fun onMessage(message: ReqT) {
                val (type, size) = getTypeAndSize(message)
                LOGGER.debug { "gRPC call received: $serviceName/$methodName. Message type = $type, size = $size" }
                invokeCounter.labels(pinName, serviceName, methodName).inc()
                requestBytes.labels(pinName, serviceName, methodName).inc(size.toDouble())
                super.onMessage(message)
            }
        }
    }

    companion object {
        val LOGGER = KotlinLogging.logger {}
        private fun getTypeAndSize(message: Any): Pair<String, Int> =
            message.toValue().run { messageValue.metadata.messageType to serializedSize }
    }
}
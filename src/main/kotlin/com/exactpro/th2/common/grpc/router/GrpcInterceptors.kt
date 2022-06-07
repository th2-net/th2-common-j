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
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ForwardingClientCall
import io.grpc.ForwardingClientCallListener
import io.grpc.ForwardingServerCall
import io.grpc.ForwardingServerCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.prometheus.client.Counter
import mu.KotlinLogging

class GrpcInterceptor(
    private val pinName: String,
    private val methodInvokeCounter: Counter,
    private val requestBytesCounter: Counter,
    private val responseBytesCounter: Counter
) : ClientInterceptor, ServerInterceptor {
    override fun <ReqT : Any, RespT : Any> interceptCall(
        method: MethodDescriptor<ReqT, RespT>,
        callOptions: CallOptions,
        next: Channel
    ): ClientCall<ReqT, RespT> {

        val fullName = method.fullMethodName
        val idx = fullName.lastIndexOf('/').coerceAtLeast(0)
        val serviceName = fullName.substring(0, idx)
        val methodName = fullName.substring(idx + 1)

        return object :
            ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            override fun sendMessage(message: ReqT) {
                val size = message.toValue().serializedSize
                LOGGER.debug { "gRPC call $serviceName/$methodName invoked on pin '$pinName'. Message size = $size" }
                methodInvokeCounter.labels(pinName, serviceName, methodName).inc()
                requestBytesCounter.labels(pinName, serviceName, methodName).inc(size.toDouble())
            }

            override fun start(responseListener: Listener<RespT>?, headers: Metadata?) {

                val forwardingListener =
                    object : ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                        override fun onMessage(message: RespT) {
                            val size = message.toValue().serializedSize
                            LOGGER.debug { "gRPC $serviceName/$methodName response received on pin '$pinName'. Message size = $size" }
                            responseBytesCounter.labels(pinName, serviceName, methodName).inc(size.toDouble())
                            super.onMessage(message)
                        }
                    }

                super.start(forwardingListener, headers)
            }
        }
    }

    override fun <ReqT : Any, RespT : Any> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {

        val fullName = call.methodDescriptor.fullMethodName
        val idx = fullName.lastIndexOf('/').coerceAtLeast(0)
        val serviceName = fullName.substring(0, idx)
        val methodName = fullName.substring(idx + 1)

        val forwardingCall = object : ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
            override fun sendMessage(message: RespT) {
                val size = message.toValue().serializedSize
                LOGGER.debug { "gRPC $serviceName/$methodName response message sent on '$pinName'. Message size = $size" }
                responseBytesCounter.labels(pinName, serviceName, methodName).inc(size.toDouble())
                super.sendMessage(message)
            }
        }

        val listener = next.startCall(forwardingCall, headers)

        return object : ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            override fun onMessage(message: ReqT) {
                val size = message.toValue().serializedSize
                LOGGER.debug { "gRPC call received on '$pinName': $serviceName/$methodName. Message size = $size" }
                methodInvokeCounter.labels(pinName, serviceName, methodName).inc()
                requestBytesCounter.labels(pinName, serviceName, methodName).inc(size.toDouble())
                super.onMessage(message)
            }
        }
    }

    companion object {
        val LOGGER = KotlinLogging.logger {}
    }
}
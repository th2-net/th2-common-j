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

import io.grpc.ForwardingServerCall
import io.grpc.ForwardingServerCallListener
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.prometheus.client.Counter
import java.util.function.Function

class ServerGrpcInterceptor (
    pinName: String,
    private val methodInvokeFunc: Function<MethodDetails, Counter.Child>,
    private val methodReceiveFunc: Function<MethodDetails, Counter.Child>,
    private val requestSizeFunc: Function<MethodDetails, Counter.Child?>,
    private val responseSizeFunc: Function<MethodDetails, Counter.Child?>,
) : AbstractGrpcInterceptor(pinName), ServerInterceptor {

    override fun <ReqT : Any, RespT : Any> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>
    ): ServerCall.Listener<ReqT> {

        val methodDetails = call.methodDescriptor.toMethodDetails(pinName)

        val forwardingCall = object : ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
            override fun sendMessage(message: RespT) {
                message.publishMetrics(
                    "gRPC ${methodDetails.serviceName}/${methodDetails.methodName} response message sent on '$pinName'",
                    responseSizeFunc.apply(methodDetails),
                    methodInvokeFunc.apply(methodDetails)
                )
                super.sendMessage(message)
            }
        }

        val listener = next.startCall(forwardingCall, headers)

        return object : ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            override fun onMessage(message: ReqT) {
                message.publishMetrics(
                    "gRPC call received on '$pinName': ${methodDetails.serviceName}/${methodDetails.methodName}",
                    requestSizeFunc.apply(methodDetails),
                    methodReceiveFunc.apply(methodDetails)
                )
                super.onMessage(message)
            }
        }
    }
}
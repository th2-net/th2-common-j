/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ForwardingClientCall
import io.grpc.ForwardingClientCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.prometheus.client.Counter
import java.util.function.Function

class ClientGrpcInterceptor(
    pinName: String,
    private val methodInvokeFunc: Function<MethodDetails, Counter.Child>,
    private val methodReceiveFunc: Function<MethodDetails, Counter.Child>,
    private val requestSizeFunc: Function<MethodDetails, Counter.Child?>,
    private val responseSizeFunc: Function<MethodDetails, Counter.Child?>,
) : AbstractGrpcInterceptor(pinName), ClientInterceptor {
    override fun <ReqT : Any, RespT : Any> interceptCall(
        method: MethodDescriptor<ReqT, RespT>,
        callOptions: CallOptions,
        next: Channel
    ): ClientCall<ReqT, RespT> {

        val methodDetails = method.toMethodDetails(pinName)

        return object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            override fun sendMessage(message: ReqT) {
                message.publishMetrics(
                    "gRPC call ${methodDetails.serviceName}/${methodDetails.methodName} invoked on pin '$pinName'",
                    requestSizeFunc.apply(methodDetails),
                    methodInvokeFunc.apply(methodDetails)
                )
                super.sendMessage(message)
            }

            override fun start(responseListener: Listener<RespT>?, headers: Metadata?) {
                val forwardingListener =
                    object : ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                        override fun onMessage(message: RespT) {
                            message.publishMetrics(
                                "gRPC ${methodDetails.serviceName}/${methodDetails.methodName} response received on pin '$pinName'",
                                responseSizeFunc.apply(methodDetails),
                                methodReceiveFunc.apply(methodDetails)
                            )
                            super.onMessage(message)
                        }
                    }
                super.start(forwardingListener, headers)
            }
        }
    }
}
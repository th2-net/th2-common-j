/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.grpc.configuration

import com.exactpro.th2.common.schema.configuration.Configuration
import com.exactpro.th2.common.schema.strategy.route.RoutingStrategy
import com.exactpro.th2.service.RetryPolicy
import com.fasterxml.jackson.annotation.JsonProperty

data class GrpcConfiguration(
    @JsonProperty var services: Map<String, GrpcServiceConfiguration> = emptyMap(),
    @JsonProperty(value = "server") var serverConfiguration: GrpcServerConfiguration = GrpcServerConfiguration(),
    @JsonProperty var retryConfiguration: GrpcRetryConfiguration = GrpcRetryConfiguration()
) : Configuration()

data class GrpcServiceConfiguration(
    @JsonProperty(required = true) var strategy: RoutingStrategy<*>? = null,
    @JsonProperty(value = "service-class", required = true) var serviceClass: Class<*>? = null,
    @JsonProperty(required = true) var endpoints: Map<String, GrpcEndpointConfiguration> = emptyMap()
) : Configuration()

data class GrpcEndpointConfiguration(
    @JsonProperty(required = true) var host: String? = null,
    @JsonProperty(required = true) var port: Int? = null,
    var attributes: List<String?> = emptyList()
) : Configuration()

data class GrpcRetryConfiguration(
    private var maxAttempts: Int = 5,
    var minMethodRetriesTimeout: Long = 100,
    var maxMethodRetriesTimeout: Long = 2000
) : Configuration(), RetryPolicy {
    override fun getDelay(index: Int): Long =
        (minMethodRetriesTimeout + if (maxAttempts > 1) (maxMethodRetriesTimeout - minMethodRetriesTimeout) / (maxAttempts - 1) * index else 0)

    override fun getMaxAttempts(): Int = maxAttempts
    fun setMaxAttempts(maxAttempts: Int) {
        this.maxAttempts = maxAttempts
    }
}

data class GrpcServerConfiguration(
    @JsonProperty(required = true) var host: String? = null,
    @JsonProperty(required = true) var port: Int = 0
) : Configuration()
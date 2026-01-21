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

package com.exactpro.th2.common.schema.grpc.configuration

import com.exactpro.th2.common.schema.configuration.Configuration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.strategy.route.RoutingStrategy
import com.fasterxml.jackson.annotation.JsonProperty
import io.grpc.internal.GrpcUtil

data class GrpcConfiguration(
    @JsonProperty var services: Map<String, GrpcServiceConfiguration> = emptyMap(),
    @JsonProperty(value = "server") var serverConfiguration: GrpcServerConfiguration = GrpcServerConfiguration(),
    @JsonProperty var maxMessageSize: Int = GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE,
) : Configuration()

data class GrpcServiceConfiguration(
    @Deprecated("For removal since v3.37") @JsonProperty(required = true) var strategy: RoutingStrategy<*>,
    @JsonProperty(required = true, value = "service-class") var serviceClass: Class<*>,
    @JsonProperty(required = true) var endpoints: Map<String, GrpcEndpointConfiguration> = emptyMap(),
    @JsonProperty var filters: List<Filter> = emptyList()
) : Configuration()

data class Filter(
    @JsonProperty(required = true) var properties: List<FieldFilterConfiguration>,
) : Configuration()

data class GrpcEndpointConfiguration(
    @JsonProperty(required = true) var host: String,
    @JsonProperty(required = true) var port: Int = 8080,
    var attributes: List<String?> = emptyList(),
) : Configuration()

data class GrpcServerConfiguration(
    var host: String? = "localhost",
    @JsonProperty(required = true) var port: Int = 8080,
    var workers: Int = 5
) : Configuration()
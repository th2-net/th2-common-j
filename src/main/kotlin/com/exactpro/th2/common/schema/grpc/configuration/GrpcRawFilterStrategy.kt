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
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.util.MultiMapFiltersDeserializer
import com.exactpro.th2.common.util.emptyMultiMap
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.commons.collections4.MultiValuedMap

data class GrpcRawFilterStrategy(var filters: List<GrpcRouterFilterConfiguration?> = emptyList())

data class GrpcRouterFilterConfiguration(
    @JsonProperty(required = true) var endpoint: String,
    @JsonDeserialize(using = MultiMapFiltersDeserializer::class) override var metadata: MultiValuedMap<String, FieldFilterConfiguration> = emptyMultiMap(),
    @JsonDeserialize(using = MultiMapFiltersDeserializer::class) override var message: MultiValuedMap<String, FieldFilterConfiguration> = emptyMultiMap()
) : RouterFilter, Configuration() {

    @JsonGetter("metadata")
    fun getJsonMetadata(): Collection<FieldFilterConfiguration> = metadata.values()

    @JsonGetter("message")
    fun getJsonMessage(): Collection<FieldFilterConfiguration> = message.values()

}
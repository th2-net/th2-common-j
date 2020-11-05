/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.grpc.configuration;

import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Collections;
import java.util.Map;

@Data
public class GrpcRouterFilterConfiguration implements RouterFilter {

    @JsonProperty(required = true)
    private String endpoint;

    @JsonProperty
    private Map<String, FieldFilterConfiguration> metadata = Collections.emptyMap();

    @JsonProperty
    private Map<String, FieldFilterConfiguration> message = Collections.emptyMap();

}
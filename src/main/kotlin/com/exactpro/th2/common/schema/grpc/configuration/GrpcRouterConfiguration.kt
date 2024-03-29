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
import io.grpc.internal.GrpcUtil

data class GrpcRouterConfiguration(
    var enableSizeMeasuring: Boolean = false,
    var keepAliveInterval: Long = 60L,
    var maxMessageSize: Int = GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE,
    var retryConfiguration: GrpcRetryConfiguration = GrpcRetryConfiguration(),
) : Configuration()
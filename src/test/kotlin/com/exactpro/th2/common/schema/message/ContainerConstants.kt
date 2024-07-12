/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message

import org.testcontainers.utility.DockerImageName
import java.time.Duration

object ContainerConstants {
    @JvmField val RABBITMQ_IMAGE_NAME: DockerImageName = DockerImageName.parse("rabbitmq:3.13.4-management-alpine")
    const val ROUTING_KEY = "routingKey"
    const val QUEUE_NAME = "queue"
    const val EXCHANGE = "test-exchange"

    const val DEFAULT_PREFETCH_COUNT = 10
    @JvmField val DEFAULT_CONFIRMATION_TIMEOUT: Duration = Duration.ofSeconds(1)
}
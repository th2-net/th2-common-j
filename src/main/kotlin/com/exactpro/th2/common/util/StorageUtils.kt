/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

@file:JvmName("StorageUtils")

package com.exactpro.th2.common.util

import com.exactpro.th2.common.grpc.Direction
import com.google.protobuf.TimestampOrBuilder
import java.time.Instant

//FIXME: this code should be extracted to a separate library together with cradle extraction.

fun Direction.toCradleDirection(): com.exactpro.cradle.Direction = when (this) {
    Direction.FIRST -> com.exactpro.cradle.Direction.FIRST
    Direction.SECOND -> com.exactpro.cradle.Direction.SECOND
    else -> throw IllegalArgumentException("Unknown the direction type '$this'")
}

fun TimestampOrBuilder.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())

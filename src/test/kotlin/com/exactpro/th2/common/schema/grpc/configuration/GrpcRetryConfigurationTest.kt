/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class GrpcRetryConfigurationTest {

    @ParameterizedTest
    @CsvSource(
        "0,0,0",
        "0,0,100",
        "0,100,100",
        "0,100,2000",
        "1,0,0",
        "1,0,100",
        "1,100,100",
        "1,100,2000",
        "100,0,0",
        "100,0,100",
        "100,100,100",
        "100,100,2000",
    )
    fun `get delay test`(maxAttempts: String, minTimeout: String, maxTimeout: String) {
        val retryPolicy = GrpcRetryConfiguration(maxAttempts.toInt(), minTimeout.toLong(), maxTimeout.toLong())

        for (attempt in -1..retryPolicy.getMaxAttempts() + 1) {
            val delay = retryPolicy.getDelay(attempt)
            when {
                attempt <= 0 -> {
                    assertEquals(retryPolicy.minMethodRetriesTimeout, delay) {
                        "Check minimum equality, delay: $delay, attempt: $attempt"
                    }
                }
                attempt >= retryPolicy.maxMethodRetriesTimeout -> {
                   assertEquals(retryPolicy.maxMethodRetriesTimeout, delay) {
                       "Check maximum equality, delay: $delay, attempt: $attempt"
                   }
                }
                else -> {
                    assertTrue(delay >= retryPolicy.minMethodRetriesTimeout) {
                        "Check minimum limit, delay: $delay, attempt: $attempt"
                    }
                    assertTrue(delay <= retryPolicy.maxMethodRetriesTimeout) {
                        "Check maximum limit, delay: $delay, attempt: $attempt"
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource(
        "-1,0,0",
        "0,-1,0",
        "0,0,-1",
        "0,1,0",
    )
    fun `negative test`(maxAttempts: String, minTimeout: String, maxTimeout: String) {
        assertThrows(IllegalArgumentException::class.java) {
            GrpcRetryConfiguration(maxAttempts.toInt(), minTimeout.toLong(), maxTimeout.toLong())
        }
    }
}
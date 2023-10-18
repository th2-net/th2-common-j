/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.service.RetryPolicy

data class GrpcRetryConfiguration(
    private var maxAttempts: Int = 60,
    var minMethodRetriesTimeout: Long = 100,
    var maxMethodRetriesTimeout: Long = 120_000
) : Configuration(), RetryPolicy {

    init {
        require(maxAttempts >= 0) {
            "'max attempts' of ${javaClass.simpleName} class must be 0 or positive"
        }
        require(minMethodRetriesTimeout >= 0) {
            "'min method retries timeout' of ${javaClass.simpleName} class must be 0 or positive"
        }
        require(maxMethodRetriesTimeout >= 0) {
            "'max method retries timeout' of ${javaClass.simpleName} class must be 0 or positive"
        }
        require(maxMethodRetriesTimeout >= minMethodRetriesTimeout) {
            "'max method retries timeout' of ${javaClass.simpleName} class must be greater of equal 'min method retries timeout'"
        }
    }

    override fun getDelay(index: Int): Long {
        val attempt = if (index > 0) {
            if (index > maxAttempts) maxAttempts else index
        } else { 0 }
        var increment = 0L
        if (maxAttempts > 1) {
            increment = (maxMethodRetriesTimeout - minMethodRetriesTimeout) / (maxAttempts - 1) * attempt
        }

        return minMethodRetriesTimeout + increment
    }

    override fun getMaxAttempts(): Int = maxAttempts
}
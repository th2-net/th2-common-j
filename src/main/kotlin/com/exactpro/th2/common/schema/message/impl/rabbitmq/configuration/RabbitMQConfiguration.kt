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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration

import com.exactpro.th2.common.schema.configuration.Configuration
import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Duration
import java.util.concurrent.ThreadLocalRandom

data class RabbitMQConfiguration(
    @JsonProperty(required = true) val host: String,
    @JsonProperty(required = true) @get:JsonProperty("vHost") val vHost: String,
    @JsonProperty(required = true) val port: Int = 5672,
    @JsonProperty(required = true) val username: String,
    @JsonProperty(required = true) val password: String,
    @Deprecated(message = "Please use subscriber name from ConnectionManagerConfiguration")
    val subscriberName: String? = null,  //FIXME: Remove in future version
    val exchangeName: String? = null
) : Configuration()

data class ConnectionManagerConfiguration(
    val subscriberName: String? = null,
    val connectionTimeout: Int = -1,
    val connectionCloseTimeout: Int = 10000,
    val maxRecoveryAttempts: Int = 5,
    val minConnectionRecoveryTimeout: Int = 10000,
    val maxConnectionRecoveryTimeout: Int = 60000,
    val prefetchCount: Int = 10,
    val retryTimeDeviationPercent: Int = 10,
    val messageRecursionLimit: Int = 100,
    val workingThreads: Int = 1,
    val confirmationTimeout: Duration = Duration.ofMinutes(5),
    val enablePublisherConfirmation: Boolean = false,
    // Default value 50MB is taken based on measurement done in ConnectionManualBenchmark class
    val maxInflightPublicationsBytes: Int = 50 * 1024 * 1024,
    val heartbeatIntervalSeconds: Int = DEFAULT_HB_INTERVAL_SECONDS,
) : Configuration() {
    init {
        check(maxRecoveryAttempts > 0) { "expected 'maxRecoveryAttempts' greater than 0 but was $maxRecoveryAttempts" }
        check(minConnectionRecoveryTimeout > 0) { "expected 'minConnectionRecoveryTimeout' greater than 0 but was $minConnectionRecoveryTimeout" }
        check(maxConnectionRecoveryTimeout >= minConnectionRecoveryTimeout) { "expected 'maxConnectionRecoveryTimeout' ($maxConnectionRecoveryTimeout) no less than 'minConnectionRecoveryTimeout' ($minConnectionRecoveryTimeout)" }
        check(retryTimeDeviationPercent in 0..100) { "expected 'retryTimeDeviationPercent' no less than 0 and not greater than 100 but was $retryTimeDeviationPercent" }
        check(workingThreads > 0) { "expected 'workingThreads' greater than 0 but was $workingThreads" }
        check(!confirmationTimeout.run { isNegative || isZero }) { "expected 'confirmationTimeout' greater than 0 but was $confirmationTimeout" }
    }

    fun createRetryingDelaySequence(): Sequence<RetryingDelay> {
        return generateSequence(RetryingDelay(0, minConnectionRecoveryTimeout)) {
            RetryingDelay(it.tryNumber + 1, RetryingDelay.getRecoveryDelay(
                it.tryNumber + 1,
                minConnectionRecoveryTimeout,
                maxConnectionRecoveryTimeout,
                maxRecoveryAttempts,
                retryTimeDeviationPercent
            ))
        }
    }

    companion object {
        const val NO_LIMIT_INFLIGHT_REQUESTS: Int = -1
        const val DEFAULT_HB_INTERVAL_SECONDS: Int = 0
    }
}

data class RetryingDelay(val tryNumber: Int, val delay: Int) {
    companion object {
        @JvmStatic
        fun getRecoveryDelay(
            numberOfTries: Int,
            minTime: Int,
            maxTime: Int,
            maxRecoveryAttempts: Int,
            deviationPercent: Int
        ): Int {
            return if (numberOfTries <= maxRecoveryAttempts) {
                getRecoveryDelayWithIncrement(numberOfTries, minTime, maxTime, maxRecoveryAttempts, deviationPercent)
            } else {
                getRecoveryDelayWithDeviation(maxTime, deviationPercent)
            }
        }

        private fun getRecoveryDelayWithDeviation(maxTime: Int, deviationPercent: Int): Int {
            val recoveryDelay: Int
            val deviation = maxTime * deviationPercent / 100
            recoveryDelay = ThreadLocalRandom.current().nextInt(maxTime - deviation, maxTime + deviation + 1)
            return recoveryDelay
        }

        private fun getRecoveryDelayWithIncrement(
            numberOfTries: Int,
            minTime: Int,
            maxTime: Int,
            maxRecoveryAttempts: Int,
            deviationPercent: Int
        ): Int {
            val delay = minTime + (maxTime - minTime) / maxRecoveryAttempts * numberOfTries
            val deviation = maxTime * deviationPercent / 100
            return ThreadLocalRandom.current().nextInt(delay - deviation, delay + deviation + 1)
        }
    }
}
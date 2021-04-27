/******************************************************************************
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
 ******************************************************************************/
@file:JvmName("CommonMetrics")

package com.exactpro.th2.common.metrics

import io.prometheus.client.Gauge
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This is a set of default baskets in millisecond for Prometheus histogram metric.
 */
@JvmField
val DEFAULT_BUCKETS = doubleArrayOf(0.000_25, 0.000_5, 0.001, 0.005, 0.010, 0.015, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0)

const val DEFAULT_LABEL_NAME : String = "session_alias"

private val LIVENESS = Gauge.build("th2_liveness", "Service liveness").register()
private val READINESS = Gauge.build("th2_readiness", "Service readiness").register()

private val RABBITMQ_READINESS = AtomicBoolean(true)
private val GRPC_READINESS = AtomicBoolean(true)
private val ALL_READINESS = CopyOnWriteArrayList(listOf(RABBITMQ_READINESS, GRPC_READINESS))

var liveness: Boolean
    get() = LIVENESS.get() == 0.0
    set(value) = LIVENESS.set(if (value) 1.0 else 0.0)

var readiness: Boolean
    get() = READINESS.get() == 0.0
    set(value) = READINESS.set(if (value) 1.0 else 0.0)

fun updateCommonReadiness() {
    var isReadness = true
    for (readinessParameter in ALL_READINESS) {
        if (!readinessParameter.get()) {
            isReadness = false
            break
        }
    }

    readiness = isReadness
}

fun addReadinessParameter(parameter: AtomicBoolean) {
    ALL_READINESS.add(parameter)
}

fun setRabbitMQReadiness(value: Boolean) {
    RABBITMQ_READINESS.set(value)
    updateCommonReadiness()
}

fun setGRPCReadiness(value: Boolean) {
    GRPC_READINESS.set(value)
    updateCommonReadiness()
}
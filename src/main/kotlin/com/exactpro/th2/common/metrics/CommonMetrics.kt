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

private val LIVENESS = Gauge.build("th2_liveness", "Service liveness").register()
private val READINESS = Gauge.build("th2_readiness", "Service readiness").register()

private val RABBITMQ_READINESS = AtomicBoolean(true)
private val GRPC_READINESS = AtomicBoolean(true)
private val ALL_READINESS = CopyOnWriteArrayList(listOf(RABBITMQ_READINESS, GRPC_READINESS))

val LIVENESS_ARBITER = MetricArbiter(LIVENESS)
val READINESS_ARBITER = MetricArbiter(READINESS)

val LIVENESS_MONITOR = LIVENESS_ARBITER.register("user_liveness")
val READINESS_MONITOR = READINESS_ARBITER.register("user_readiness")

var liveness: Boolean
    get() = LIVENESS_MONITOR.isEnabled
    set(value) = if (value) LIVENESS_MONITOR.enable() else LIVENESS_MONITOR.disable()

var readiness: Boolean
    get() = READINESS_MONITOR.isEnabled
    set(value) = if (value) READINESS_MONITOR.enable() else READINESS_MONITOR.disable()

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

class HealthMetrics(val livenessMonitor: MetricArbiter.MetricMonitor,
                    val readinessMonitor: MetricArbiter.MetricMonitor)
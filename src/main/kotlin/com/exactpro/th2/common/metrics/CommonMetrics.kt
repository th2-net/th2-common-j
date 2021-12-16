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

@file:JvmName("CommonMetrics")

package com.exactpro.th2.common.metrics

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * This is a set of default baskets in millisecond for Prometheus histogram metric.
 */
@JvmField
val DEFAULT_BUCKETS = doubleArrayOf(0.000_25, 0.000_5, 0.001, 0.005, 0.010, 0.015, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0)

const val TH2_PIN_LABEL = "th2_pin"
const val TH2_TYPE_LABEL = "th2_type"
const val EXCHANGE_LABEL = "exchange"
const val QUEUE_LABEL = "queue"
const val ROUTING_KEY_LABEL = "routing_key"
const val SESSION_ALIAS_LABEL = "session_alias"
const val DIRECTION_LABEL = "direction"
const val MESSAGE_TYPE_LABEL = "message_type"

const val LIVENESS = "liveness"
const val READINESS = "readiness"

private val LIVENESS_ARBITER = AggregatingMetric(listOf(PrometheusMetric("th2_$LIVENESS", "Service $LIVENESS"), FileMetric( "healthy")))
private val READINESS_ARBITER = AggregatingMetric(listOf(PrometheusMetric("th2_$READINESS", "Service $READINESS"), FileMetric( "ready")))

private val RABBITMQ_READINESS = AtomicBoolean(true)
private val GRPC_READINESS = AtomicBoolean(true)
private val ALL_READINESS = CopyOnWriteArrayList(listOf(RABBITMQ_READINESS, GRPC_READINESS))

fun registerLiveness(name: String) = LIVENESS_ARBITER.createMonitor("${name}_$LIVENESS")
fun registerReadiness(name: String) = READINESS_ARBITER.createMonitor("${name}_$READINESS")

fun registerLiveness(obj: Any) = LIVENESS_ARBITER.createMonitor("${obj::class.simpleName}_${LIVENESS}_${obj.hashCode()}")
fun registerReadiness(obj: Any) = READINESS_ARBITER.createMonitor("${obj::class.simpleName}_${READINESS}_${obj.hashCode()}")

@JvmField
val LIVENESS_MONITOR = registerLiveness("user")
@JvmField
val READINESS_MONITOR = registerReadiness("user")

/**
 * @see registerLiveness
 * @see MetricMonitor
 */
@Deprecated(message = "Please create yours monitor with registerLiveness and use one")
var liveness: Boolean
    get() = LIVENESS_MONITOR.isEnabled
    set(value) = if (value) LIVENESS_MONITOR.enable() else LIVENESS_MONITOR.disable()

/**
 * @see registerReadiness
 * @see MetricMonitor
 */
@Deprecated(message = "Please create yours monitor with registerReadiness and use one")
var readiness: Boolean
    get() = READINESS_MONITOR.isEnabled
    set(value) = if (value) READINESS_MONITOR.enable() else READINESS_MONITOR.disable()

/**
 * @see registerLiveness
 * @see registerReadiness
 * @see MetricMonitor
 */
@Deprecated(message = "Please create yours monitors with registerLiveness and registerReadiness ans use ones")
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

/**
 * @see registerReadiness
 * @see MetricMonitor
 */
@Deprecated(message = "Please create yours monitor with registerReadiness and use one")
fun addReadinessParameter(parameter: AtomicBoolean) {
    ALL_READINESS.add(parameter)
}

/**
 * @see registerReadiness
 * @see MetricMonitor
 */
@Deprecated(message = "Please create yours monitor with registerReadiness and use one")
fun setRabbitMQReadiness(value: Boolean) {
    RABBITMQ_READINESS.set(value)
    updateCommonReadiness()
}

/**
 * @see registerReadiness
 * @see MetricMonitor
 */
@Deprecated(message = "Please create yours monitor with registerReadiness and use one")
fun setGRPCReadiness(value: Boolean) {
    GRPC_READINESS.set(value)
    updateCommonReadiness()
}

class HealthMetrics @JvmOverloads constructor(
    val livenessMonitor: MetricMonitor,
    val readinessMonitor: MetricMonitor,
    private val maxAttempts: Int = 10
) {
    @JvmOverloads
    constructor(parent: Any, attempts: Int = 10) : this(registerLiveness(parent), registerReadiness(parent), attempts)

    @JvmOverloads
    constructor(name: String, attempts: Int = 10) : this(registerLiveness(name), registerReadiness(name), attempts)

    private val attempts = AtomicInteger(0)

    fun notReady() = when {
        maxAttempts > attempts.incrementAndGet() -> readinessMonitor.disable()
        else -> disable()
    }

    fun enable() {
        livenessMonitor.enable()
        readinessMonitor.enable()
    }

    fun disable() {
        livenessMonitor.disable()
        readinessMonitor.disable()
    }
}
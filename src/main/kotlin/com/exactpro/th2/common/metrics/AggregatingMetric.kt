/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.metrics

/**
 * Metric which aggregates several other metrics
 */
class AggregatingMetric(private val metrics: List<Metric>) : Metric {
    override val isEnabled: Boolean
        get() = !metrics.any { !it.isEnabled }

    override fun createMonitor(name: String): MetricMonitor = MetricMonitor(this, name)

    override fun isEnabled(monitor: MetricMonitor): Boolean = metrics.all { it.isEnabled(monitor) }

    override fun enable(monitor: MetricMonitor) = metrics.forEach {
        it.enable(monitor)
    }

    override fun disable(monitor: MetricMonitor) = metrics.forEach {
        it.disable(monitor)
    }
}
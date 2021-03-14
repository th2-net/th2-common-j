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
package com.exactpro.th2.common.metrics

import io.prometheus.client.Gauge
import java.util.concurrent.ConcurrentHashMap

open class MetricArbiter(name: String, help: String) {
    private val metric: Gauge = Gauge.build(name, help).register()
    private val disabledMonitors: MutableSet<MetricMonitor> = ConcurrentHashMap.newKeySet()

    var isEnabled: Boolean = false
        protected set(value) {
            if (field != value) {
                field = value
                metricChangedValue(value)
            }
        }

    fun register(name: String): MetricMonitor {
        return MetricMonitor(this, name)
    }

    /**
     * Is passed monitor enabled
     */
    fun isMonitorEnabled(monitor: MetricMonitor) = !disabledMonitors.contains(monitor)

    /**
     * Enable monitor, that mean all is ok
     */
    fun enable(monitor: MetricMonitor) {
        disabledMonitors.remove(monitor)
        isEnabled = disabledMonitors.isEmpty() // Set value in all cases because the initialised value is false
    }

    /**
     * Disable monitor, that mean something wrong
     */
    fun disable(monitor: MetricMonitor) {
        if (disabledMonitors.add(monitor)) {
            isEnabled = false
        }
    }

    protected open fun metricChangedValue(value: Boolean) {
        metric.set(if (value) 1.0 else 0.0)
    }
}
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

import java.util.concurrent.ConcurrentHashMap

abstract class AbstractMetric : Metric {

    private val disabledMonitors: MutableSet<MetricMonitor> = ConcurrentHashMap.newKeySet()

    override val isEnabled: Boolean
        get() = disabledMonitors.isEmpty()

    override fun createMonitor(name: String): MetricMonitor = MetricMonitor(this, name)

    override fun enable(monitor: MetricMonitor) {
        if(disabledMonitors.remove(monitor)) {
            onValueChange(disabledMonitors.isEmpty())
        }
    }

    override fun disable(monitor: MetricMonitor) {
        if (disabledMonitors.add(monitor)) {
            onValueChange(false)
        }
    }

    override fun isEnabled(monitor: MetricMonitor): Boolean = monitor !in disabledMonitors

    protected abstract fun onValueChange(value: Boolean)
}
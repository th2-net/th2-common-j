/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

/**
 * Entity which allows to have multiple monitors for a single metric.
 * Resulting metric status will be calculated from statuses of all monitors
 * @see MetricMonitor
 */
interface Metric {
    /**
     * Current metric status.
     */
    val isEnabled: Boolean

    /**
     * Creates new monitor for this metric
     * @param name monitor name
     * @return monitor which can change metrics value
     */
    fun createMonitor(name: String) : MetricMonitor

    /**
     * Checks if status of a provided [monitor] is `enabled`
     */
    fun isEnabled(monitor: MetricMonitor) : Boolean

    /**
     * Changes status of a provided [monitor] to `enabled`
     * Returns `true` if previous status was `disabled`, otherwise `false`
     */
    fun enable(monitor: MetricMonitor): Boolean

    /**
     * Changes status of a provided [monitor] to `disabled`
     * Returns `true` if previous status was `enabled`, otherwise `false`
     */
    fun disable(monitor: MetricMonitor): Boolean
}
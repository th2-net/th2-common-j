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

/**
 * Interface for managing logic metrics with multiple monitors which can change metrics value
 * @see MetricMonitor
 */
interface MetricArbiter {
    /**
     * Status property of the current arbiter.
     */
    val isEnabled: Boolean

    /**
     * Register new endpoint
     * @param name monitor name
     * @return monitor which can change metrics value
     */
    fun register(name: String) : MetricMonitor

    /**
     * Check if monitor is enable
     */
    fun isMonitorEnabled(monitor: MetricMonitor) : Boolean

    /**
     * Enable monitor
     */
    fun enable(monitor: MetricMonitor)

    /**
     * Disable monitor
     */
    fun disable(monitor: MetricMonitor)
}
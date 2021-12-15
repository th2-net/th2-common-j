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

import mu.KotlinLogging
import org.apache.commons.lang3.builder.ToStringBuilder

/**
 * Class for change state of metric
 */
class MetricMonitor(
    private val arbiter: Metric,
    val name: String
) {
    /**
     * Status property of the current monitor. Please use the [isMetricEnabled] method to get cumulative metric status
     */
    var isEnabled: Boolean
        get() = arbiter.isEnabled(this)
        set(value) = if (value) enable() else disable()

    /**
     * Cumulative metric status
     */
    val isMetricEnabled: Boolean
        get() = arbiter.isEnabled

    /**
     * Changes status of this monitor to `enabled`
     */
    fun enable() {
        val wasDisabled = !arbiter.isEnabled
        arbiter.enable(this)
        if (wasDisabled) {
            LOGGER.info { "$name is enabled" }
        }
    }

    /**
     * Changes status of this monitor to `disabled`
     */
    fun disable() {
        val wasEnabled = arbiter.isEnabled
        arbiter.disable(this)
        if (wasEnabled) {
            LOGGER.info { "$name is disabled" }
        }
    }


    override fun toString(): String {
        return ToStringBuilder(this)
            .append("name", name)
            .toString()
    }
    
    companion object {
        val LOGGER = KotlinLogging.logger {}
    }
}
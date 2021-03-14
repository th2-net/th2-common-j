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

import org.apache.commons.lang3.builder.ToStringBuilder
import java.util.concurrent.atomic.AtomicInteger

/**
 * Class for change state of metric
 */
class MetricMonitor(
    private val arbiter: MetricArbiter,
    val name: String
) {
    private val id: Int = COUNTER.incrementAndGet()

    /**
     * Status property of the current monitor. Please use the [isMetricEnabled] method to get cumulative metric status
     */
    var isEnabled: Boolean
        get() = arbiter.isMonitorEnabled(this)
        set(value) = if (value) enable() else disable()

    /**
     * Cumulative metric status
     */
    val isMetricEnabled: Boolean
        get() = arbiter.isEnabled

    /**
     * Enable monitor, that mean all is ok
     */
    fun enable() {
        arbiter.enable(this)
    }

    /**
     * Disable monitor, that mean something wrong
     */
    fun disable() {
        arbiter.disable(this)
    }


    override fun toString(): String {
        return ToStringBuilder(this)
            .append("name", name)
            .append("id", id)
            .toString()
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other?.javaClass != javaClass) return false

        other as MetricMonitor
        return id == other.id
    }

    companion object {
        private val COUNTER = AtomicInteger(0)
    }
}
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

package com.exactpro.th2.common.metrics;

import io.prometheus.client.Gauge;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricArbiter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricArbiter.class);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);
    private static final Path DEFAULT_PATH_TO_METRIC_FOLDER = Path.of("/var/th2/metrics");

    private final Set<Integer> ids;
    private final Gauge prometheusMetric;
    private final File metricFile;

    public MetricArbiter(Gauge metric) {
        this.ids = ConcurrentHashMap.newKeySet();
        this.prometheusMetric = metric;
        this.metricFile = DEFAULT_PATH_TO_METRIC_FOLDER.resolve(metric.describe().get(0).name).toFile();
        if (this.metricFile.exists() && !this.metricFile.delete()) {
            throw new IllegalStateException("Can not delete file = " + metricFile);
        }
    }

    private void setMetricValue(boolean value) {
        prometheusMetric.set(value ? 1.0 : 0.0);

        if (value) {
            try {
                metricFile.createNewFile();
            } catch (Exception e) {
                throw new IllegalStateException("Can not create file = " + metricFile, e);
            }
        } else {
            metricFile.delete();
        }
    }

    private void calculateMetricValue() {
        setMetricValue(ids.isEmpty());
    }

    public boolean getMetricValue() { return prometheusMetric.get() == 1.0; }

    public MetricMonitor register(String name) {
        int id = COUNTER.incrementAndGet();
        return new MetricMonitor(id, name);
    }

    /**
     * Class for change state of metric
     */
    public class MetricMonitor {
        private String name;
        private final int id;

        private MetricMonitor(int id, String name) {
            this.name = name;
            this.id = id;
        }

        /**
         * Unregister this monitor
         */
        public void unregister() {
            if (ids.remove(id)) {
                calculateMetricValue();
            } else {
                LOGGER.warn("Metric monitor with id {} is already unregistered", id);
            }
        }

        /**
         * Enable monitor, that mean all is ok
         */
        public void enable() {
            if (ids.remove(id)) {
                calculateMetricValue();
            }
        }

        /**
         * Disable monitor, that mean something wrong
         */
        public void disable() {
            if (ids.add(id)) {
                setMetricValue(false);
            }
        }

        /**
         * @return monitors status
         */
        public boolean isEnabled() {
            return ids.contains(id);
        }

        /**
         * @return monitors name
         */
        public String getName() {
            return name;
        }

        /**
         * Set new monitor name
         * @param name new name
         */
        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("name", name)
                    .append("id", id)
                    .toString();
        }
    }
}

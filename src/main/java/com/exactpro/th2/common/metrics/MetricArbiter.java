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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricArbiter {
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private ConcurrentMap<Integer, AtomicBoolean> map;
    private final Gauge prometheusMetric;

    public MetricArbiter(Gauge metric) {
        this.map = new ConcurrentHashMap<>();
        this.prometheusMetric = metric;
    }

    private void setMetricValue(boolean value) {
        prometheusMetric.set(value ? 1.0 : 0.0);
    }

    private void calculateMetricValue() {
        for (AtomicBoolean value : map.values()) {
            if(!value.get()) {
                setMetricValue(false);
                return;
            }
        }

        setMetricValue(true);
    }

    public boolean getMetricValue() { return prometheusMetric.get() == 1.0; }

    public MetricMonitor register(String name) {
        int id = COUNTER.incrementAndGet();
        AtomicBoolean value = new AtomicBoolean(true);
        map.put(id, value);

        return new MetricMonitor(id, name, value);
    }

    public class MetricMonitor {
        private String name;
        private AtomicBoolean value;
        private int id;

        private MetricMonitor(int id, String name, AtomicBoolean value) {
            this.name = name;
            this.value = value;
            this.id = id;
        }

        public void unregister(int id) {
            AtomicBoolean value = map.remove(id);

            if(value != null) {
                calculateMetricValue();
            } else {
                throw new IllegalArgumentException(String.format("No mapping for key %s", id));
            }
        }

        public void enable() {
            if(!value.getAndSet(true)) {
                calculateMetricValue();
            }
        }

        public void disable() {
            if(value.getAndSet(false)) {
                setMetricValue(false);
            }
        }

        public boolean isEnabled() {
            return value.get();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getId() {
            return id;
        }

        @Override
        public String toString() {
            return "MetricArbiter{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
}

package com.exactpro.th2.common.metrics;

import io.prometheus.client.Gauge;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricArbiter {
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private ConcurrentHashMap<Integer, AtomicBoolean> map;
    private Gauge prometheusMetric;

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
        MetricMonitor monitor = new MetricMonitor(id, name, value);

        return monitor;
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

        public void unregister(Integer id) {
            boolean value = map.get(id).get();
            map.remove(id);

            if(!value) {
                calculateMetricValue();
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
    }
}

package com.exactpro.th2.common.metrics;

import io.prometheus.client.Gauge;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricArbiter {
    private ConcurrentHashMap<Integer, AtomicBoolean> map;
    private AtomicInteger counter;
    private AtomicBoolean commonMetricValue;
    Gauge metric;

    public MetricArbiter(Gauge metric) {
        this.map = new ConcurrentHashMap<>();
        this.counter = new AtomicInteger(0);
        this.commonMetricValue = new AtomicBoolean(true);
        this.metric = metric;
    }

    private void setValue(Integer key, AtomicBoolean value) {
        map.replace(key, value);
    }

    public AtomicBoolean getCommonMetricValue() { return commonMetricValue; }

    private void calculateCommonMetricValue() {
        commonMetricValue.set(true);

        map.forEach((key, value) -> {
            if(!value.get()) {
                commonMetricValue.set(false);
            }
        });
    }

    public MetricMonitor register(String name) {
        counter.incrementAndGet();
        AtomicBoolean value = new AtomicBoolean(true);
        map.put(counter.get(), value);
        MetricMonitor monitor = new MetricMonitor(name, value);

        return monitor;
    }

    public class MetricMonitor {
        private String name;
        private AtomicBoolean value;
        private Integer id;

        private MetricMonitor(String name, AtomicBoolean value) {
            this.name = name;
            this.value = value;
            id = counter.get();
        }

        public void unregister(Integer id) {
            boolean value = map.get(id).get();
            map.remove(id);

            if(!value) {
                calculateCommonMetricValue();
            }
        }

        public void enable() {
            if(!value.get()) {
                value.set(true);
                setValue(id, value);
                calculateCommonMetricValue();
            }
        }

        public void disable() {
            if(value.get()) {
                value.set(false);
                setValue(id, value);
                commonMetricValue.set(false);
            }
        }

        public AtomicBoolean isEnabled() {
            return value;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}

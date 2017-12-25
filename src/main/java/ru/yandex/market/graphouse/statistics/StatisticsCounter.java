package ru.yandex.market.graphouse.statistics;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.market.graphouse.Metric;
import ru.yandex.market.graphouse.cacher.MetricCacher;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Nikolay Firov <a href="mailto:firov@yandex-team.ru"></a>
 * @date 22.12.17
 */
public class StatisticsCounter {
    private static final Logger log = LogManager.getLogger();

    private final String prefix;
    private final Integer flushPeriodSeconds;

    private final MetricCacher metricCacher;
    private final MetricSearch metricSearch;

    private final Map<AccumulatedMetric, MetricDescription> accumulatedMetricsDescriptions = new HashMap<>();
    private final Map<InstantMetric, MetricDescription> instantMetricsDescriptions = new HashMap<>();
    private final Map<AccumulatedMetric, AtomicDouble> metricsCounters = new HashMap<>();

    public StatisticsCounter(String prefix, Integer flushPeriodSeconds,
                             MetricSearch metricSearch, MetricCacher metricCacher) {
        this.prefix = prefix;
        this.flushPeriodSeconds = flushPeriodSeconds;
        this.metricCacher = metricCacher;
        this.metricSearch = metricSearch;

        Arrays.stream(AccumulatedMetric.values()).forEach(metric -> metricsCounters.put(metric, new AtomicDouble()));
    }

    public void initialize() {
        for (AccumulatedMetric metric : AccumulatedMetric.values()) {
            String name = String.format("%s.accumulated.%s", prefix, metric.name().toLowerCase());
            loadMetric(name, description -> accumulatedMetricsDescriptions.put(metric, description));
        }

        for (InstantMetric metric : InstantMetric.values()) {
            String name = String.format("%s.instant.%s", prefix, metric.name().toLowerCase());
            loadMetric(name, description -> instantMetricsDescriptions.put(metric, description));
        }
    }

    public void accumulateMetric(AccumulatedMetric metric, double value) {
        this.metricsCounters.get(metric).addAndGet(value);
    }

    public void flush(Map<InstantMetric, Supplier<Double>> instantMetricsSuppliers) {
        int timestampSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

        List<Metric> metrics = getAccumulatedMetrics(timestampSeconds);
        metrics.addAll(getInstantMetrics(instantMetricsSuppliers, timestampSeconds));

        this.metricCacher.submitMetrics(metrics);
    }

    private void loadMetric(String name, Consumer<MetricDescription> save) {
        MetricDescription description = this.metricSearch.add(name);

        if (description != null) {
            save.accept(description);
            log.info("Statistics metric loaded " + name);
        } else {
            log.warn("Failed to load metric " + name);
        }
    }

    private List<Metric> getInstantMetrics(Map<InstantMetric, Supplier<Double>> instantMetricsSuppliers,
                                           int timestampSeconds) {
        List<Metric> metrics = new ArrayList<>(instantMetricsSuppliers.size());

        for (Map.Entry<InstantMetric, Supplier<Double>> entry : instantMetricsSuppliers.entrySet()) {
            MetricDescription description = this.instantMetricsDescriptions.get(entry.getKey());
            if (description == null) {
                continue;
            }

            metrics.add(new Metric(description, timestampSeconds, entry.getValue().get(), timestampSeconds));
        }

        return metrics;
    }

    private List<Metric> getAccumulatedMetrics(int timestampSeconds) {
        List<Metric> metrics = new ArrayList<>(this.metricsCounters.size());

        for (Map.Entry<AccumulatedMetric, AtomicDouble> counter : this.metricsCounters.entrySet()) {
            Double value = counter.getValue().getAndSet(0);
            MetricDescription description = this.accumulatedMetricsDescriptions.get(counter.getKey());
            if (description == null) {
                continue;
            }

            metrics.add(new Metric(description, timestampSeconds, value, timestampSeconds));
        }

        return metrics;
    }

    public int getFlushPeriodSeconds() {
        return this.flushPeriodSeconds;
    }
}

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

    private final Map<AccumulatedMetric, MetricDescription> metricsDescriptions = new HashMap<>();
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
            MetricDescription description = this.metricSearch.add(name);

            if (description != null) {
                metricsDescriptions.put(metric, description);
                log.info("Statistics metric loaded " + name);
            } else {
                log.warn("Failed to load metric " + name);
            }
        }
    }

    public void accumulateMetric(AccumulatedMetric metric, double value) {
        this.metricsCounters.get(metric).addAndGet(value);
    }

    public void flush() {
        List<Metric> metrics = new ArrayList<>(metricsCounters.size());

        for (Map.Entry<AccumulatedMetric, AtomicDouble> counter : metricsCounters.entrySet()) {
            Double value = counter.getValue().getAndSet(0);
            MetricDescription description = metricsDescriptions.get(counter.getKey());
            if (description == null) {
                continue;
            }

            int timestampSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            metrics.add(new Metric(description, timestampSeconds, value, timestampSeconds));
        }

        this.metricCacher.submitMetrics(metrics);
    }

    public int getFlushPeriodSeconds() {
        return this.flushPeriodSeconds;
    }
}

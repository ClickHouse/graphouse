package ru.yandex.market.graphouse.server;

import com.google.common.base.Splitter;
import org.apache.logging.log4j.Logger;
import ru.yandex.market.graphouse.Metric;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.util.List;

public abstract class BaseMetricFactory implements MetricFactory {

    protected final MetricValidator metricValidator;
    private final boolean redirectHostMetrics;
    private final String hostMetricDir;
    private final List<String> hostPostfixes;

    public BaseMetricFactory(MetricValidator metricValidator, boolean redirectHostMetrics, String hostMetricDir, String hostPostfixes) {
        this.metricValidator = metricValidator;
        this.redirectHostMetrics = redirectHostMetrics;
        this.hostMetricDir = hostMetricDir;
        this.hostPostfixes = Splitter.on(',').omitEmptyStrings().splitToList(hostPostfixes);
        if (redirectHostMetrics) {
            getLog().info("Host host metrics redirection enabled for postfixes {} to dir {}", hostPostfixes, hostMetricDir);
        } else {
            getLog().info("Host metric redirection disabled");
        }
    }

    protected abstract Logger getLog();

    /**
     * Validates the metric and, if successful, creates or updates the current one.
     *
     * @param line           contains name of the metric, value, timestamp
     * @param updatedSeconds
     * @return Created or updated metric,
     * <code>null</code> if the metric name or value is not valid, the metric is banned
     */
    @Override
    public Metric createMetric(String line, int updatedSeconds) {

        String[] splits = line.split(" ");
        if (splits.length != 3) {
            return null;
        }
        String name = processName(splits[0]);

        try {
            MetricDescription metricDescription = getOrCreateMetricDescription(name);
            if (metricDescription == null) {
                return null;
            }

            return createMetric(metricDescription, splits, updatedSeconds);
        } catch (RuntimeException e) {
            getLog().error("Error on get MetricDescription for line '" + line + "'. Data may be lost.", e);
            return null;
        }
    }

    protected abstract MetricDescription getOrCreateMetricDescription(String name);

    private Metric createMetric(MetricDescription metric, String[] metricParts, int updatedSeconds) {
        try {
            double value = Double.parseDouble(metricParts[1]);
            if (!Double.isFinite(value)) {
                return null;
            }
            int timeSeconds = (int) Math.round(Double.parseDouble(metricParts[2]));
            if (timeSeconds <= 0) {
                return null;
            }
            return new Metric(metric, timeSeconds, value, updatedSeconds);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    protected String processName(String name) {
        if (!redirectHostMetrics) {
            return name;
        }
        String[] splits = name.split("\\.", 3);
        for (String hostPostfix : hostPostfixes) {
            if (splits[1].endsWith(hostPostfix)) {
                return splits[0] + "." + hostMetricDir + "." + splits[1] + "." + splits[2];
            }
        }
        return name;
    }
}

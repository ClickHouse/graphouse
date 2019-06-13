package ru.yandex.market.graphouse.server;

import com.google.common.base.Splitter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.market.graphouse.Metric;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.util.List;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 08/05/15
 */
public class MetricFactory {

    private static final Logger log = LogManager.getLogger();

    private final MetricSearch metricSearch;
    private final MetricValidator metricValidator;

    private final boolean redirectHostMetrics;
    private final String hostMetricDir;
    private final List<String> hostPostfixes;

    public MetricFactory(MetricSearch metricSearch, MetricValidator metricValidator,
                         boolean redirectHostMetrics, String hostMetricDir, String hostPostfixes) {
        this.metricSearch = metricSearch;
        this.metricValidator = metricValidator;
        this.redirectHostMetrics = redirectHostMetrics;
        this.hostMetricDir = hostMetricDir;
        this.hostPostfixes = Splitter.on(',').omitEmptyStrings().splitToList(hostPostfixes);
        if (redirectHostMetrics) {
            log.info("Host host metrics redirection enabled for postfixes {} to dir {}", hostPostfixes, hostMetricDir);
        } else {
            log.info("Host metric redirection disabled");
        }
    }

    /**
     * Validates the metric and, if successful, creates or updates the current one.
     *
     * @param line           contains name of the metric, value, timestamp
     * @param updatedSeconds
     * @return Created or updated metric,
     * <code>null</code> if the metric name or value is not valid, the metric is banned
     */
    public Metric createMetric(String line, int updatedSeconds) {

        String[] splits = line.split(" ");
        if (splits.length != 3) {
            return null;
        }
        String name = processName(splits[0]);
        String[] nameSplits = MetricUtil.splitToLevels(name);
        //Trying to fast find metric in tree. In success we can skip validation;
        MetricDescription metric = metricSearch.maybeFindMetric(nameSplits);
        if (metric == null) {
            if (!metricValidator.validate(name, false)) {
                return null;
            }
            metric = metricSearch.add(name);
            if (metric == null) {
                return null;
            }
        } else if (metric.getStatus() == MetricStatus.AUTO_HIDDEN || metric.getStatus() == MetricStatus.HIDDEN) {
            metric = metricSearch.add(name);
        }
        if (metric.getStatus() == MetricStatus.BAN) {
            return null;
        }

        try {
            double value = Double.parseDouble(splits[1]);
            if (!Double.isFinite(value)) {
                return null;
            }
            int timeSeconds = (int) Math.round(Double.parseDouble(splits[2]));
            if (timeSeconds <= 0) {
                return null;
            }
            return new Metric(metric, timeSeconds, value, updatedSeconds);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public String processName(String name) {
        if (!redirectHostMetrics) {
            return name;
        }
        String[] splits = name.split("\\.", 3);
        for (int i = 0; i < hostPostfixes.size(); i++) {
            if (splits[1].endsWith(hostPostfixes.get(i))) {
                return splits[0] + "." + hostMetricDir + "." + splits[1] + "." + splits[2];
            }
        }
        return name;
    }
}

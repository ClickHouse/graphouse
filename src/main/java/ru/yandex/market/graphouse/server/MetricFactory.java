package ru.yandex.market.graphouse.server;

import org.springframework.beans.factory.annotation.Value;
import ru.yandex.market.graphouse.Metric;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 08/05/15
 */
public class MetricFactory {

    private final MetricSearch metricSearch;
    private final MetricValidator metricValidator;

    @Value("${graphouse.host-metric-redirect.enabled}")
    private boolean redirectHostMetrics = true;

    @Value("${graphouse.host-metric-redirect.dir}")
    private String hostMetricDir = "HOST";

    @Value("${graphouse.host-metric-redirect.postfixes}")
    private List<String> hostPostfixes = Collections.emptyList();

    public MetricFactory(MetricSearch metricSearch, MetricValidator metricValidator) {
        this.metricSearch = metricSearch;
        this.metricValidator = metricValidator;
    }

    /**
     * Валидирует метрику и в случае успеха создаёт или обновляет текущую.
     *
     * @param line           через пробел название метрики, значение, метка времени
     * @param updatedSeconds
     * @return созданную или обновленную метрику,
     * <code>null</code> если название метрики или значение не валидны, метрика забанена
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
            int timeSeconds = Integer.valueOf(splits[2]);
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

    public void setRedirectHostMetrics(boolean redirectHostMetrics) {
        this.redirectHostMetrics = redirectHostMetrics;
    }

    public void setHostMetricDir(String hostMetricDir) {
        this.hostMetricDir = hostMetricDir;
    }

    public void setHostPostfixes(String hostPostfixes) {
        this.hostPostfixes = Arrays.asList(hostPostfixes.split(","));
    }
}

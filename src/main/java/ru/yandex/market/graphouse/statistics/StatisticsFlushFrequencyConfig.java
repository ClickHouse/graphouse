package ru.yandex.market.graphouse.statistics;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Nikolay Firov <a href="mailto:firov@yandex-team.ru"></a>
 * @date 22.12.17
 */
public class StatisticsFlushFrequencyConfig {
    private final Map<String, Integer> metricNameToFlushPeriodInSeconds = new HashMap<>();

    public StatisticsFlushFrequencyConfig(String config) {
        if (config == null || config.isEmpty())
            return;

        for (String metricConfig : config.split(",")) {
            String[] pair = metricConfig.split(":", 2);
            metricNameToFlushPeriodInSeconds.put(pair[0], Integer.parseInt(pair[1]));
        }
    }

    public Map<String, Integer> getMetricNameToFlushPeriodInSeconds() {
        return metricNameToFlushPeriodInSeconds;
    }
}

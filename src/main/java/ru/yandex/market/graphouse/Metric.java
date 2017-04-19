package ru.yandex.market.graphouse;

import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.util.Date;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 06/04/15
 */
public class Metric {
    private final MetricDescription metricDescription;
    private final int timestampSeconds;
    private final double value;
    private final int updatedSeconds;

    public Metric(MetricDescription metricDescription, int timestampSeconds, double value, int updatedSeconds) {
        this.metricDescription = metricDescription;
        this.timestampSeconds = timestampSeconds;
        this.value = value;
        this.updatedSeconds = updatedSeconds;
    }

    public MetricDescription getMetricDescription() {
        return metricDescription;
    }


    public int getTimestampSeconds() {
        return timestampSeconds;
    }

    public double getValue() {
        return value;
    }

    public int getUpdatedSeconds() {
        return updatedSeconds;
    }
}

package ru.yandex.market.graphouse.data;

import ru.yandex.market.graphouse.search.tree.MetricName;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 16/11/2018
 */
public class MetricDataQueryParams {
    private final int startTimeSeconds;
    private final int endTimeSeconds;
    private final int stepSeconds;

    public MetricDataQueryParams(int startTimeSeconds, int endTimeSeconds, int stepSeconds) {
        this.startTimeSeconds = startTimeSeconds;
        this.endTimeSeconds = endTimeSeconds;
        this.stepSeconds = stepSeconds;
    }

    public static MetricDataQueryParams create(List<MetricName> metrics, int startTimeSeconds, int endTimeSeconds,
                                               int maxPointsPerMetric) {
        int ageSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - startTimeSeconds;
        int stepSeconds = metrics.stream()
            .mapToInt(m -> m.getRetention().getStepSize(ageSeconds))
            .max()
            .orElse(1);

        int timeSeconds = endTimeSeconds - startTimeSeconds;
        int dataPoints = timeSeconds / stepSeconds;
        if (maxPointsPerMetric > 0 && dataPoints > maxPointsPerMetric) {
            int ratio = (dataPoints + maxPointsPerMetric - 1) / maxPointsPerMetric; //Divide with round up (ceil)
            stepSeconds = stepSeconds * ratio;
            dataPoints = timeSeconds / stepSeconds;
        }
        startTimeSeconds = startTimeSeconds / stepSeconds * stepSeconds;
        endTimeSeconds = startTimeSeconds + (dataPoints * stepSeconds);
        return new MetricDataQueryParams(startTimeSeconds, endTimeSeconds, stepSeconds);
    }

    public int getStartTimeSeconds() {
        return startTimeSeconds;
    }

    public int getEndTimeSeconds() {
        return endTimeSeconds;
    }

    public int getStepSeconds() {
        return stepSeconds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetricDataQueryParams that = (MetricDataQueryParams) o;
        return startTimeSeconds == that.startTimeSeconds &&
            endTimeSeconds == that.endTimeSeconds &&
            stepSeconds == that.stepSeconds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTimeSeconds, endTimeSeconds, stepSeconds);
    }

    @Override
    public String toString() {
        return "MetricDataQueryParams{" +
            "startTimeSeconds=" + startTimeSeconds +
            ", endTimeSeconds=" + endTimeSeconds +
            ", stepSeconds=" + stepSeconds +
            '}';
    }
}

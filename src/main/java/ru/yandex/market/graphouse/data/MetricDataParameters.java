package ru.yandex.market.graphouse.data;

import java.util.List;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 02.11.16
 */
public class MetricDataParameters {
    private final List<String> metrics;
    private final int startTimeSeconds;
    private final int endTimeSeconds;

    private final MetricStep metricStep;
    private final int pointsCount;

    private String reqKey;

    public MetricDataParameters(List<String> metrics, int startTimeSeconds, int endTimeSeconds) {
        this.metrics = metrics;

        this.metricStep = MetricStep.fromMetric(metrics.get(0));
        this.startTimeSeconds = startTimeSeconds - startTimeSeconds % metricStep.getStepSizeInSeconds();
        this.endTimeSeconds = endTimeSeconds - endTimeSeconds % metricStep.getStepSizeInSeconds();

        this.pointsCount = (this.endTimeSeconds - this.startTimeSeconds) / metricStep.getStepSizeInSeconds() + 1;
    }

    public String getFirstMetric() {
        return metrics.get(0);
    }

    public List<String> getMetrics() {
        return metrics;
    }

    public int getStartTimeSeconds() {
        return startTimeSeconds;
    }

    public int getEndTimeSeconds() {
        return endTimeSeconds;
    }

    public String getReqKey() {
        return reqKey;
    }

    public MetricStep getMetricStep() {
        return metricStep;
    }

    public boolean isMultiMetrics() {
        return metrics.size() > 1;
    }

    public int getPointsCount() {
        return pointsCount;
    }

    public void setReqKey(String reqKey) {
        this.reqKey = reqKey;
    }

    @Override
    public String toString() {
        return "MetricDataParameters{" +
            "metrics=" + metrics +
            ", startTimeSeconds=" + startTimeSeconds +
            ", endTimeSeconds=" + endTimeSeconds +
            ", metricStep=" + metricStep +
            ", pointsCount=" + pointsCount +
            ", reqKey='" + reqKey + '\'' +
            '}';
    }
}
package ru.yandex.market.graphouse.data;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 01.11.16
 */
enum MetricStep {
    ONE_SEC("one_sec.", 1),
    FIVE_SEC("five_sec.", 5 * ONE_SEC.stepSizeInSeconds),
    ONE_MIN("one_min.", 60 * ONE_SEC.stepSizeInSeconds),
    FIVE_MIN("five_min.", 5 * ONE_MIN.stepSizeInSeconds),
    TEN_MIN("ten_min.", 10 * ONE_MIN.stepSizeInSeconds),
    ONE_HOUR("one_hour.", 60 * ONE_MIN.stepSizeInSeconds),
    ONE_DAY("one_day.", 24 * ONE_HOUR.stepSizeInSeconds);

    private final int stepSizeInSeconds;

    private final String pattern;

    MetricStep(String pattern, int stepSizeInSeconds) {
        this.pattern = pattern;
        this.stepSizeInSeconds = stepSizeInSeconds;
    }

    public static MetricStep fromMetric(String metric) {

        for (MetricStep metricStep : MetricStep.values()) {
            if (metric.startsWith(metricStep.pattern)) {
                return metricStep;
            }
        }

        return ONE_HOUR;
    }

    public int getStepSizeInSeconds() {
        return stepSizeInSeconds;
    }
}

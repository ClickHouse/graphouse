package ru.yandex.market.graphouse.retention;

import java.util.regex.Pattern;

public class MetricRetentionConfig {
    private final Pattern regexp;
    private final boolean isDefault;
    private final MetricRetention metricRetention;

    public enum Type {
        RETENTION(1), AGGREGATION(2), ALL(3);

        private final int t;
        Type(int t) { this.t = t; }
    }

    public MetricRetentionConfig(String regexp, boolean isDefault, MetricRetention metricRetention) {
        this.regexp = Pattern.compile(regexp);
        this.isDefault = isDefault;
        this.metricRetention = metricRetention;
    }

    @Override
    public String toString() {
        return "Main regexp: " + regexp + "; Function: " + metricRetention.getFunction() +
            "; Ranges: " + metricRetention.getRanges().toString() + "; Is default: " + isDefault;
    }

    public String getRegexp() {
        return regexp.toString();
    }

    public boolean getIsDefault() {
        return isDefault;
    }

    public Type getType() {
        if (metricRetention.getFunction().equals("")) {
            return Type.RETENTION;
        }
        if (metricRetention.getRanges().asMapOfRanges().isEmpty()) {
            return Type.AGGREGATION;
        }
        return Type.ALL;
    }

    boolean matches(String name) {
        if (isDefault) {
            return true;
        }

        return regexp.matcher(name).matches();
    }

    MetricRetention getMetricRetention() {
        return metricRetention;
    }
}

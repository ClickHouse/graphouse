package ru.yandex.market.graphouse;

import java.util.regex.Pattern;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 27/05/15
 */
public class MetricValidator {

    private static final int MAX_ALLOWED_METRIC_LENGTH = 255; //Cause VARCHAR шin mysql

    private int minMetricLength = 10;
    private int maxMetricLength = 200;
    private int minDots = 2;
    private int maxDots = 15;

    private String metricRegexp = "^(one_sec|five_sec|one_min|five_min|one_hour|one_day)\\.[-_0-9a-zA-Z\\.]+$";
    private Pattern metricPattern = Pattern.compile(metricRegexp);

    public static final MetricValidator DEFAULT = new MetricValidator();

    public boolean validate(String name) {
        return validate(name, false);
    }

    public boolean validate(String name, boolean allowDirs) {
        if (name.length() < minMetricLength || name.length() > maxMetricLength) {
            return false;
        }
        if (!validateDots(name, allowDirs)) {
            return false;
        }
        return metricPattern.matcher(name).matches();
    }

    private boolean validateDots(String name, boolean allowDirs) {
        if (name.charAt(0) == '.') {
            return false;
        }
        if (!allowDirs && name.charAt(name.length() - 1) == '.') {
            return false;
        }
        int prevDotIndex = -1;
        int dotIndex = -1;
        int dotCount = 0;
        while ((dotIndex = name.indexOf('.', prevDotIndex + 1)) > 0) {
            if (prevDotIndex + 1 == dotIndex) {
                return false; //Две точки подряд
            }
            prevDotIndex = dotIndex;
            dotCount++;
        }
        if (dotCount < minDots || dotCount > maxDots) {
            return false;
        }
        return true;
    }

    public void setMinMetricLength(int minMetricLength) {
        this.minMetricLength = minMetricLength;
    }

    public void setMaxMetricLength(int maxMetricLength) {
        if (maxMetricLength > MAX_ALLOWED_METRIC_LENGTH) {
            throw new IllegalArgumentException("maxMetricLength show be mot more then " + MAX_ALLOWED_METRIC_LENGTH);
        }
        this.maxMetricLength = maxMetricLength;
    }

    public void setMinDots(int minDots) {
        this.minDots = minDots;
    }

    public void setMaxDots(int maxDots) {
        this.maxDots = maxDots;
    }

    public void setMetricRegexp(String metricRegexp) {
        this.metricRegexp = metricRegexp;
        this.metricPattern = Pattern.compile(metricRegexp);
    }
}


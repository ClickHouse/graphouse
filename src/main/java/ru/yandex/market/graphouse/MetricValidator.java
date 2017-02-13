package ru.yandex.market.graphouse;

import org.springframework.beans.factory.annotation.Value;

import java.util.regex.Pattern;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 27/05/15
 */
public class MetricValidator {

    @Value("${graphouse.metric-validation.min-length}")
    private int minMetricLength = 10;

    @Value("${graphouse.metric-validation.max-length}")
    private int maxMetricLength = 200;

    @Value("${graphouse.metric-validation.min-dots}")
    private int minDots = 2;

    @Value("${graphouse.metric-validation.max-dots}")
    private int maxDots = 15;

    @Value("${graphouse.metric-validation.regexp}")
    private String metricRegexp = "^(one_sec|five_sec|one_min|five_min|one_hour|one_day)\\.[-_0-9a-zA-Z\\.]*$";

    private Pattern metricPattern = Pattern.compile(metricRegexp);

    public static final MetricValidator DEFAULT = new MetricValidator();

    public boolean validate(String name, boolean allowDirs) {
        boolean isDir = MetricUtil.isDir(name);
        if ((!isDir && name.length() < minMetricLength) || name.length() > maxMetricLength) {
            return false;
        }
        if (!validateDots(name, allowDirs, isDir)) {
            return false;
        }
        return metricPattern.matcher(name).matches();
    }

    private boolean validateDots(String name, boolean allowDirs, boolean isDir) {
        if (name.charAt(0) == '.') {
            return false;
        }
        if (!allowDirs && isDir) {
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

        if ((!isDir && dotCount < minDots) || dotCount > maxDots) {
            return false;
        }
        return true;
    }
}


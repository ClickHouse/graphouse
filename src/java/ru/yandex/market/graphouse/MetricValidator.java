package ru.yandex.market.graphouse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 06/04/15
 */
public class MetricValidator {
    public static final int MIN_METRIC_LENGTH = 10;
    public static final int MAX_METRIC_LENGTH = 200;
    private static final int MIN_DOTS = 2;
    private static final int MAX_DOTS = 10;

    private static final String METRIC_REGEXP = "^(five_sec|one_min|five_min|one_hour|one_day)\\.[-_0-9a-zA-Z\\.]+$";
    private static final Pattern METRIC_PATTERN = Pattern.compile(METRIC_REGEXP);

    public static boolean validate(String name) {
        if (name.length() < MIN_METRIC_LENGTH && name.length() > MAX_METRIC_LENGTH) {
            return false;
        }
        if (!validateDots(name)) {
            return false;
        }
        return METRIC_PATTERN.matcher(name).matches();
    }

    private static boolean validateDots(String name) {
        if (name.charAt(0) == '.' || name.charAt(name.length() - 1) == '.') {
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
        if (dotCount < MIN_DOTS || dotCount > MAX_DOTS) {
            return false;
        }
        return true;
    }
}

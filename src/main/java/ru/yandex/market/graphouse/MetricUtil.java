package ru.yandex.market.graphouse;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 27/01/2017
 */
public class MetricUtil {
    public static final char LEVEL_SPLITTER = '.';

    public static boolean isDir(String metric) {
        return metric.charAt(metric.length() - 1) == LEVEL_SPLITTER;
    }

    public static String[] splitToLevels(String metric) {
        return metric.split("\\.");
    }

}

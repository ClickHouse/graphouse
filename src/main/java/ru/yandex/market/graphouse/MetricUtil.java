package ru.yandex.market.graphouse;

import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    public static String getLastLevelName(String metric) {
        if (isDir(metric)) {
            return metric.substring(metric.lastIndexOf(LEVEL_SPLITTER, metric.length() - 2) + 1, metric.length() - 1);
        } else {
            return metric.substring(metric.lastIndexOf(LEVEL_SPLITTER) + 1);
        }
    }

    public static String getParentName(String metric) {
        return metric.substring(0, metric.lastIndexOf(LEVEL_SPLITTER, metric.length() - 2) + 1);
    }

    public static int getLevel(String metric) {
        int splitsCount = StringUtils.countOccurrencesOf(metric, ".");
        if (isDir(metric)) {
            return splitsCount;
        }
        return splitsCount + 1;
    }

    public static Pattern createStartWithDirectoryPattern(String[] directories) {
        String directoriesPattern = Arrays.stream(directories)
            .map(Pattern::quote)
            .collect(Collectors.joining("|"));
        return Pattern.compile("^(" + directoriesPattern + ")");
    }
}

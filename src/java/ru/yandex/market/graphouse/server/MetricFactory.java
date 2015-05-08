package ru.yandex.market.graphouse.server;

import org.springframework.beans.factory.annotation.Required;
import ru.yandex.market.graphouse.Metric;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.MetricStatus;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 08/05/15
 */
public class MetricFactory {

    public static final int MIN_METRIC_LENGTH = 10;
    public static final int MAX_METRIC_LENGTH = 200;
    private static final int MIN_DOTS = 2;
    private static final int MAX_DOTS = 10;

    private static final String METRIC_REGEXP = "^(five_sec|one_min|five_min|one_hour|one_day)\\.[-_0-9a-zA-Z\\.]+$";
    private static final Pattern METRIC_PATTERN = Pattern.compile(METRIC_REGEXP);


    private MetricSearch metricSearch;

    private boolean redirectHostMetrics = true;
    private String hostMetricDir = "HOST";
    private List<String> hostPostfixes = Arrays.asList("yandex_net", "yandex_ru");

    public Metric createMetric(String line, Date currentDate) {

        String[] splits = line.split(" ");
        if (splits.length != 3) {
            return null;
        }
        String name = splits[0];
        if (!validate(name)) {
            return null;
        }
        try {
            double value = Double.parseDouble(splits[1]);
            int time = Integer.valueOf(splits[2]);
            if (time <= 0) {
                return null;
            }
            name = processName(name);
            MetricStatus status = metricSearch.add(name);
            if (status == MetricStatus.NEW || status == MetricStatus.EXISTING) {
                return new Metric(name, time, value, currentDate);
            } else {
                return null;
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public String processName(String name) {
        if (!redirectHostMetrics) {
            return name;
        }
        String[] splits = name.split("\\.", 3);
        for (int i = 0; i < hostPostfixes.size(); i++) {
            if (splits[1].endsWith(hostPostfixes.get(i))) {
                return splits[0] + "." + hostMetricDir + "." + splits[1] + "." + splits[2];
            }
        }
        return name;
    }

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

    @Required
    public void setMetricSearch(MetricSearch metricSearch) {
        this.metricSearch = metricSearch;
    }

    public void setRedirectHostMetrics(boolean redirectHostMetrics) {
        this.redirectHostMetrics = redirectHostMetrics;
    }

    public void setHostMetricDir(String hostMetricDir) {
        this.hostMetricDir = hostMetricDir;
    }

    public void setHostPostfixes(String hostPostfixes) {
        this.hostPostfixes = Arrays.asList(hostPostfixes.split(","));
    }
}

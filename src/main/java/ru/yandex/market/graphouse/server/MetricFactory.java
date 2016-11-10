package ru.yandex.market.graphouse.server;

import org.springframework.beans.factory.annotation.Required;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.Metric;
import ru.yandex.market.graphouse.search.MetricDescription;
import ru.yandex.market.graphouse.search.MetricSearch;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 08/05/15
 */
public class MetricFactory {

    private MetricSearch metricSearch;
    private MetricValidator metricValidator;

    private boolean redirectHostMetrics = true;
    private String hostMetricDir = "HOST";
    private List<String> hostPostfixes = Arrays.asList("yandex_net", "yandex_ru");

    /**
     * Валидирует метрику и в случае успеха создаёт или обновляет текущую.
     *
     * @param line    через пробел название метрики, значение, метка времени
     * @param updated
     * @return созданную или обновленную метрику,
     * <code>null</code> если название метрики или значение не валидны, метрика забанена
     */
    public Metric createMetric(String line, int updated) {

        String[] splits = line.split(" ");
        if (splits.length != 3) {
            return null;
        }
        String name = splits[0];
        if (!metricValidator.validate(name)) {
            return null;
        }
        try {
            double value = Double.parseDouble(splits[1]);
            if (!Double.isFinite(value)) {
                return null;
            }
            int timeSeconds = Integer.valueOf(splits[2]);
            if (timeSeconds <= 0) {
                return null;
            }
            Date date = new Date(TimeUnit.SECONDS.toMillis(timeSeconds));
            name = processName(name);
            MetricDescription metricDescription = metricSearch.add(name);
            if (metricDescription != null) {
                return new Metric(metricDescription, date, value, updated);
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


    @Required
    public void setMetricValidator(MetricValidator metricValidator) {
        this.metricValidator = metricValidator;
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

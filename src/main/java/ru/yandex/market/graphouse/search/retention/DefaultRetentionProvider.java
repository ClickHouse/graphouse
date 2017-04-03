package ru.yandex.market.graphouse.search.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 03/04/2017
 */
public class DefaultRetentionProvider implements RetentionProvider {

    private final static Logger log = LogManager.getLogger();

    private final static int DAY = (int) TimeUnit.DAYS.toSeconds(1);
    private final static int MONTH = (int) TimeUnit.DAYS.toSeconds(30); //2592000
    private final static int YEAR = 12 * MONTH; //31104000


    public DefaultRetentionProvider() {
        fillDefaultMetricSteps();
    }

    @Override
    public MetricRetention getRetention(String metric) {
        for (MetricRetention metricRetention : retentions) {
            if (metricRetention.matches(metric)) {
                return metricRetention;
            }
        }
        throw new IllegalStateException("Retention for metric '" + metric + "' not found");

    }

    private final static List<MetricRetention> retentions = new ArrayList<>();

    private void fillDefaultMetricSteps() {

        retentions.add(
            new MetricRetention.MetricDataRetentionBuilder("^one_sec", "avg")
                .addRetention(0, 1)
                .addRetention(DAY, 5)
                .addRetention(7 * DAY, 60)
                .addRetention(3 * MONTH, 300)
                .addRetention(YEAR, 600)
                .build()
        );

        retentions.add(
            new MetricRetention.MetricDataRetentionBuilder("^five_sec", "avg")
                .addRetention(0, 5)
                .addRetention(7 * DAY, 60)
                .addRetention(3 * MONTH, 300)
                .addRetention(YEAR, 600)
                .build()
        );

        retentions.add(
            new MetricRetention.MetricDataRetentionBuilder("^one_min", "avg")
                .addRetention(0, 60)
                .addRetention(3 * MONTH, 300)
                .addRetention(YEAR, 600)
                .build()
        );

        retentions.add(
            new MetricRetention.MetricDataRetentionBuilder("^five_min", "avg")
                .addRetention(0, 300)
                .addRetention(YEAR, 600)
                .build()
        );

        retentions.add(
            new MetricRetention.MetricDataRetentionBuilder("^ten_min", "avg")
                .addRetention(0, 600)
                .build()
        );

        retentions.add(
            new MetricRetention.MetricDataRetentionBuilder("^half_hour", "avg")
                .addRetention(0, 1_800)
                .build()
        );

        retentions.add(
            new MetricRetention.MetricDataRetentionBuilder("^one_hour", "avg")
                .addRetention(0, 3_600)
                .build()
        );

        retentions.add(
            new MetricRetention.MetricDataRetentionBuilder("^one_day", "avg")
                .addRetention(0, 86_400)
                .build()
        );

        retentions.add(
            new MetricRetention.MetricDataRetentionBuilder(".*", "avg")
                .addRetention(0, 60)
                .addRetention(MONTH, 300)
                .addRetention(YEAR, 600)
                .build()
        );
    }


}

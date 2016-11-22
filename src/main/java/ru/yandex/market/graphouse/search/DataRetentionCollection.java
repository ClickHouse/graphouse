package ru.yandex.market.graphouse.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 07.11.16
 */
public class DataRetentionCollection implements InitializingBean {

    private final static Logger log = LogManager.getLogger();

    private final static int DAY = (int) TimeUnit.DAYS.toSeconds(1);
    private final static int MONTH = (int) TimeUnit.DAYS.toSeconds(30); //2592000
    private final static int YEAR = 12 * MONTH; //31104000

    private final static List<MetricDataRetention> retentions = new ArrayList<>();

    private void fillMetricSteps() {

        retentions.add(
            new MetricDataRetention.MetricDataRetentionBuilder("^one_sec", "avg")
                .addRetention(0, 1)
                .addRetention(DAY, 5)
                .addRetention(7 * DAY, 60)
                .addRetention(3 * MONTH, 300)
                .addRetention(YEAR, 600)
                .build()
        );

        retentions.add(
            new MetricDataRetention.MetricDataRetentionBuilder("^five_sec", "avg")
                .addRetention(0, 5)
                .addRetention(7 * DAY, 60)
                .addRetention(3 * MONTH, 300)
                .addRetention(YEAR, 600)
                .build()
        );

        retentions.add(
            new MetricDataRetention.MetricDataRetentionBuilder("^one_min", "avg")
                .addRetention(0, 60)
                .addRetention(3 * MONTH, 300)
                .addRetention(YEAR, 600)
                .build()
        );

        retentions.add(
            new MetricDataRetention.MetricDataRetentionBuilder("^five_min", "avg")
                .addRetention(0, 300)
                .addRetention(YEAR, 600)
                .build()
        );

        retentions.add(
            new MetricDataRetention.MetricDataRetentionBuilder("^ten_min", "avg")
                .addRetention(0, 600)
                .build()
        );

        retentions.add(
            new MetricDataRetention.MetricDataRetentionBuilder("^half_hour", "avg")
                .addRetention(0, 1_800)
                .build()
        );

        retentions.add(
            new MetricDataRetention.MetricDataRetentionBuilder("^one_hour", "avg")
                .addRetention(0, 3_600)
                .build()
        );

        retentions.add(
            new MetricDataRetention.MetricDataRetentionBuilder("^one_day", "avg")
                .addRetention(0, 86_400)
                .build()
        );

        retentions.add(
            new MetricDataRetention.MetricDataRetentionBuilder(".*", "avg")
                .addRetention(0, 60)
                .addRetention(MONTH, 300)
                .addRetention(YEAR, 600)
                .build()
        );
    }

    MetricDataRetention getDataRetention(String metricName) {
        MetricDataRetention dataRetention = null;

        for (MetricDataRetention metricDataRetention : retentions) {
            if (metricDataRetention.validateName(metricName)) {
                dataRetention = metricDataRetention;
                break;
            }
        }

        if (dataRetention == null) {
            log.warn("Retention for metric " + metricName + " not found");
        }

        return dataRetention;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        fillMetricSteps();
    }
}

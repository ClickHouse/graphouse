package ru.yandex.market.graphouse.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 03/04/2017
 */
public class DefaultRetentionProvider extends BaseRetentionProvider {

    private final static Logger log = LogManager.getLogger();

    private final static int DAY = (int) TimeUnit.DAYS.toSeconds(1);
    private final static int MONTH = (int) TimeUnit.DAYS.toSeconds(30); //2592000
    private final static int YEAR = 12 * MONTH; //31104000

    private final static List<MetricRetentionConfig> RETENTIONS = getDefault();

    public DefaultRetentionProvider() {
        super(RETENTIONS);
    }

    private static List<MetricRetentionConfig> getDefault() {

        List<MetricRetentionConfig> retentions = new ArrayList<>();

        retentions.add(
            new MetricRetentionConfig("^one_sec.*", false,
                new MetricRetention.MetricDataRetentionBuilder("avg")
                    .addRetention(0, 1)
                    .addRetention(DAY, 5)
                    .addRetention(7 * DAY, 60)
                    .addRetention(3 * MONTH, 300)
                    .addRetention(YEAR, 600)
                    .build()
            )
        );

        retentions.add(
            new MetricRetentionConfig("^five_sec.*",  false,
                new MetricRetention.MetricDataRetentionBuilder("avg")
                    .addRetention(0, 5)
                    .addRetention(7 * DAY, 60)
                    .addRetention(3 * MONTH, 300)
                    .addRetention(YEAR, 600)
                    .build()
            )
        );

        retentions.add(
            new MetricRetentionConfig("^one_min.*", false,
                new MetricRetention.MetricDataRetentionBuilder("avg")
                    .addRetention(0, 60)
                    .addRetention(3 * MONTH, 300)
                    .addRetention(YEAR, 600)
                    .build()
            )
        );

        retentions.add(
            new MetricRetentionConfig("^five_min.*", false,
                new MetricRetention.MetricDataRetentionBuilder("avg")
                    .addRetention(0, 300)
                    .addRetention(YEAR, 600)
                    .build()
            )
        );

        retentions.add(
            new MetricRetentionConfig("^ten_min.*", false,
                new MetricRetention.MetricDataRetentionBuilder("avg")
                    .addRetention(0, 600)
                    .build()
            )
        );

        retentions.add(
            new MetricRetentionConfig("^half_hour.*", false,
                new MetricRetention.MetricDataRetentionBuilder("avg")
                    .addRetention(0, 1_800)
                    .build()
            )
        );

        retentions.add(
            new MetricRetentionConfig("^one_hour.*", false,
                new MetricRetention.MetricDataRetentionBuilder("avg")
                    .addRetention(0, 3_600)
                    .build()
            )
        );

        retentions.add(
            new MetricRetentionConfig("^one_day.*", false,
                new MetricRetention.MetricDataRetentionBuilder("avg")
                    .addRetention(0, 86_400)
                    .build()
            )
        );

        retentions.add(
            new MetricRetentionConfig(".*", true,
                new MetricRetention.MetricDataRetentionBuilder("avg")
                    .addRetention(0, 60)
                    .addRetention(MONTH, 300)
                    .addRetention(YEAR, 600)
                    .build()
            )
        );

        return Collections.unmodifiableList(retentions);
    }


}

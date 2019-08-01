package ru.yandex.market.graphouse.retention;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CombinedRetentionProviderTest {
    private CombinedRetentionProvider retentionProvider;

    @Before
    public void setUp() {
        final List<MetricRetentionConfig> configRetentions = new ArrayList<>();
        configRetentions.add(
            new MetricRetentionConfig(".*^one_sec.*", false,
                new MetricRetention.MetricDataRetentionBuilder("")
                    .addRetention(0, 1)
                    .addRetention(3600, 5)
                    .addRetention(86400, 60)
                    .build()
            )
        );
        configRetentions.add(
            new MetricRetentionConfig(".*^one_hour.*", false,
                new MetricRetention.MetricDataRetentionBuilder("")
                    .addRetention(0, 3600)
                    .addRetention(31536000, 86400)
                    .build()
            )
        );
        configRetentions.add(
            new MetricRetentionConfig(".*max$.*", false,
                new MetricRetention.MetricDataRetentionBuilder("max")
                    .addRetention(0, 0)
                    .build()
            )
        );
        configRetentions.add(
            new MetricRetentionConfig(".*min$.*", false,
                new MetricRetention.MetricDataRetentionBuilder("min")
                    .addRetention(0, 0)
                    .build()
            )
        );
        configRetentions.add(
            new MetricRetentionConfig(".*^one_day.*.count$.*", false,
                new MetricRetention.MetricDataRetentionBuilder("sum")
                    .addRetention(0, 86400)
                    .build()
            )
        );
        configRetentions.add(
            new MetricRetentionConfig(".*.*", true,
                new MetricRetention.MetricDataRetentionBuilder("avg")
                    .addRetention(0, 60)
                    .addRetention(7776000, 600)
                    .addRetention(31536000, 3600)
                    .addRetention(63072000, 86400)
                    .build()
            )
        );
        retentionProvider = new CombinedRetentionProvider(configRetentions);
    }

    @Test
    public void testOneMinAvg() {
        // default
        String metric = "one_min.dir.name";
        MetricRetention retention = retentionProvider.getRetention(metric);
        Assert.assertEquals(
            "Function: avg; Ranges: " +
                "[[0..7776000)=60, [7776000..31536000)=600, [31536000..63072000)=3600, [63072000..+∞)=86400]",
            retention.toString()
        );
    }

    @Test
    public void testOneMinMax() {
        // max + default
        String metric = "one_min.dir.name.max";
        MetricRetention retention = retentionProvider.getRetention(metric);
        Assert.assertEquals(
            "Function: max; Ranges: " +
                "[[0..7776000)=60, [7776000..31536000)=600, [31536000..63072000)=3600, [63072000..+∞)=86400]",
            retention.toString()
        );
    }

    @Test
    public void testOneSecMin() {
        // one_sec + min
        String metric = "one_sec.dir.name.min";
        MetricRetention retention = retentionProvider.getRetention(metric);
        Assert.assertEquals(
            "Function: min; " +
                "Ranges: [[0..3600)=1, [3600..86400)=5, [86400..+∞)=60]",
            retention.toString()
        );
    }

    @Test
    public void testOneHourAvg() {
        // one_hour + default
        String metric = "one_hour.dir.name";
        MetricRetention retention = retentionProvider.getRetention(metric);
        Assert.assertEquals(
            "Function: avg; " +
                "Ranges: [[0..31536000)=3600, [31536000..+∞)=86400]",
            retention.toString()
        );
    }

    @Test
    public void testOneDaySum() {
        // one_day
        String metric = "one_day.dir.name.count";
        MetricRetention retention = retentionProvider.getRetention(metric);
        Assert.assertEquals(
            "Function: sum; Ranges: [[0..+∞)=86400]",
            retention.toString()
        );
    }
}

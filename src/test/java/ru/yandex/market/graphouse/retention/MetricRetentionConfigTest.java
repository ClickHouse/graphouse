package ru.yandex.market.graphouse.retention;

import org.junit.Assert;
import org.junit.Test;

public class MetricRetentionConfigTest {
    @Test
    public void testDefault() {
        MetricRetentionConfig retention = new MetricRetentionConfig(".*", true,
            new MetricRetention.MetricDataRetentionBuilder("avg")
            .addRetention(0, 60)
            .addRetention(3600, 600)
            .build()
        );
        Assert.assertEquals(
            "Main regexp: .*; Function: avg; Ranges: [[0..3600)=60, [3600..+∞)=600]; Is default: true",
            retention.toString()
        );
        Assert.assertEquals(true, retention.getIsDefault());
        Assert.assertEquals(MetricRetentionConfig.Type.ALL, retention.getType());
    }

    @Test
    public void testCreateAggregation() {
        MetricRetentionConfig aggregation = new MetricRetentionConfig(".*avg$.*", false,
            new MetricRetention.MetricDataRetentionBuilder("avg")
            .addRetention(0, 0)
            .build()
        );
        Assert.assertEquals(MetricRetentionConfig.Type.AGGREGATION, aggregation.getType());
        Assert.assertEquals("avg", aggregation.getMetricRetention().getFunction());
        Assert.assertEquals(true, aggregation.getMetricRetention().getRanges().asMapOfRanges().isEmpty());
    }

    @Test
    public void testCreateRetention() {
        MetricRetentionConfig retention = new MetricRetentionConfig(".*^start.*", false,
            new MetricRetention.MetricDataRetentionBuilder("")
            .addRetention(0, 60)
            .addRetention(3600, 600)
            .build()
        );
        Assert.assertEquals(MetricRetentionConfig.Type.RETENTION, retention.getType());
        Assert.assertEquals("", retention.getMetricRetention().getFunction());
    }

    @Test
    public void testCombinedBuilder() {
        MetricRetentionConfig aggregation = new MetricRetentionConfig(".*avg$.*", false,
            new MetricRetention.MetricDataRetentionBuilder("avg")
                .addRetention(0, 0)
                .build()
        );
        MetricRetentionConfig retention = new MetricRetentionConfig(".*^start.*", false,
        new MetricRetention.MetricDataRetentionBuilder("")
            .addRetention(0, 60)
            .addRetention(3600, 600)
            .build()
        );
        MetricRetentionConfig combined = new MetricRetentionConfig("", false,
            new MetricRetention.MetricDataRetentionBuilder(
                aggregation.getMetricRetention().getFunction()
            )
                .build(retention.getMetricRetention().getRanges())
        );
        Assert.assertEquals(MetricRetentionConfig.Type.RETENTION, retention.getType());
        Assert.assertEquals(MetricRetentionConfig.Type.AGGREGATION, aggregation.getType());
        Assert.assertEquals(MetricRetentionConfig.Type.ALL, combined.getType());
        Assert.assertEquals(retention.getMetricRetention().getRanges(), combined.getMetricRetention().getRanges());
        Assert.assertEquals(aggregation.getMetricRetention().getFunction(), combined.getMetricRetention().getFunction());
        Assert.assertEquals(
            "Main regexp: ; Function: avg; " +
                "Ranges: [[0..3600)=60, [3600..+∞)=600]; Is default: false",
            combined.toString()
        );
    }
}

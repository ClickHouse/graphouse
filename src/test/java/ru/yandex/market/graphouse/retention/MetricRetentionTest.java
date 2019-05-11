package ru.yandex.market.graphouse.retention;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.junit.Assert;
import org.junit.Test;


public class MetricRetentionTest {
    @Test
    public void testDefault() {
        MetricRetention retention = new MetricRetention.MetricDataRetentionBuilder(
            ".*",
            "avg",
            true
        )
            .addRetention(0, 60)
            .addRetention(3600, 600)
            .build();
        Assert.assertEquals(
            "Main regexp: .*; Second pattern: null; Function: avg; Ranges: [[0..3600)=60, [3600..+∞)=600]",
            retention.toString()
        );
        Assert.assertEquals(true, retention.getIsDefault());
        Assert.assertEquals(MetricRetention.Type.ALL, retention.getType());
    }

    @Test
    public void testCreateAggregation() {
        MetricRetention aggregation = new MetricRetention.MetricDataRetentionBuilder(
            ".*avg$.*",
            "avg",
            false
        )
            .addRetention(0, 0)
            .build();
        Assert.assertEquals(MetricRetention.Type.AGGREGATION, aggregation.getType());
        Assert.assertEquals("avg", aggregation.getFunction());
        Assert.assertEquals(true, aggregation.getRanges().asMapOfRanges().isEmpty());
    }

    @Test
    public void testCreateRetention() {
        MetricRetention retention = new MetricRetention.MetricDataRetentionBuilder(
            ".*^start.*",
            "",
            false
        )
            .addRetention(0, 60)
            .addRetention(3600, 600)
            .build();
        Assert.assertEquals(MetricRetention.Type.RETENTION, retention.getType());
        Assert.assertEquals("", retention.getFunction());
    }

    @Test
    public void testCombinedBuilder() {
        MetricRetention aggregation = new MetricRetention.MetricDataRetentionBuilder(
            ".*avg$.*",
            "avg",
            false
        )
            .addRetention(0, 0)
            .build();
        MetricRetention retention = new MetricRetention.MetricDataRetentionBuilder(
            ".*^start.*",
            "",
            false
        )
            .addRetention(0, 60)
            .addRetention(3600, 600)
            .build();
        MetricRetention combined = new MetricRetention.MetricDataRetentionBuilder(
            aggregation.getRegexp(),
            retention.getRegexp(),
            aggregation.getFunction()
        )
            .build(retention.getRanges());
        Assert.assertEquals(MetricRetention.Type.RETENTION, retention.getType());
        Assert.assertEquals(MetricRetention.Type.AGGREGATION, aggregation.getType());
        Assert.assertEquals(MetricRetention.Type.ALL, combined.getType());
        Assert.assertEquals(retention.getRanges(), combined.getRanges());
        Assert.assertEquals(aggregation.getFunction(), combined.getFunction());
        Assert.assertEquals(
            "Main regexp: .*avg$.*; Second pattern: .*^start.*; Function: avg; " +
                "Ranges: [[0..3600)=60, [3600..+∞)=600]",
            combined.toString()
        );
    }
}

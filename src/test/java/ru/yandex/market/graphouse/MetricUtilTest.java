package ru.yandex.market.graphouse;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 03/02/2017
 */
public class MetricUtilTest {
    @Test
    public void getLastLevelName() throws Exception {
        Assert.assertEquals("c", MetricUtil.getLastLevelName("a.b.c"));
        Assert.assertEquals("c", MetricUtil.getLastLevelName("a.b.c."));
    }

    @Test
    public void getParentNameTest() {
        Assert.assertEquals("", MetricUtil.getParentName("one_min."));
        Assert.assertEquals("one_min.", MetricUtil.getParentName("one_min.clickphite."));
        Assert.assertEquals("one_min.clickphite.", MetricUtil.getParentName("one_min.clickphite.some_metric"));
    }

    @Test
    public void getLevel() {
        Assert.assertEquals(1, MetricUtil.getLevel("one_min."));
        Assert.assertEquals(2, MetricUtil.getLevel("one_min.clickphite."));
        Assert.assertEquals(3, MetricUtil.getLevel("one_min.clickphite.some_metric"));
        Assert.assertEquals(6, MetricUtil.getLevel("one_min.market.market_api.gravicapa06e_yandex_ru.gc.endOfMajorGC"));
    }

    @Test
    public void testDirectoryPattern() {
        String directories = "one_hour.,one_day.";
        Pattern pattern = MetricUtil.createStartWithDirectoryPattern(directories.split(","));

        Assert.assertFalse(pattern.matcher("one_sec.").find());
        Assert.assertFalse(pattern.matcher("five_min.clickphite.").find());
        Assert.assertFalse(pattern.matcher("one_min.clickphite.some_metric").find());
        Assert.assertFalse(pattern.matcher("one_hours.clickphite.some_metric").find());
        Assert.assertFalse(pattern.matcher("one_min.one_day.clickphite.some_metric").find());

        Assert.assertTrue(pattern.matcher("one_hour.").find());
        Assert.assertTrue(pattern.matcher("one_day.clickphite.").find());
        Assert.assertTrue(pattern.matcher("one_day.clickphite.some_metric").find());
    }
}
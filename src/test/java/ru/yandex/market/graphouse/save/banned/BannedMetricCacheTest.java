package ru.yandex.market.graphouse.save.banned;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.market.graphouse.search.MetricStatus;

public class BannedMetricCacheTest {

    @Test
    public void addSingleBanMetricTest() {
        BannedMetricCache bannedMetricCache = new BannedMetricCache();

        bannedMetricCache.addMetricWithStatus("foo.", MetricStatus.BAN);

        Assert.assertEquals(1, bannedMetricCache.getCacheState().bannedMetricsCount);
        Assert.assertEquals(0, bannedMetricCache.getCacheState().autoBannedMetricsCount);
        Assert.assertEquals(1, bannedMetricCache.getCacheState().nodesCount);
    }

    @Test
    public void addSingleAddAutoBanMetricTest() {
        BannedMetricCache bannedMetricCache = new BannedMetricCache();

        bannedMetricCache.addMetricWithStatus("foo.", MetricStatus.AUTO_BAN);

        Assert.assertEquals(0, bannedMetricCache.getCacheState().bannedMetricsCount);
        Assert.assertEquals(1, bannedMetricCache.getCacheState().autoBannedMetricsCount);
        Assert.assertEquals(1, bannedMetricCache.getCacheState().nodesCount);
    }

    /*
    root
    |_foo. SIMPLE
    | |_bar. SIMPLE
    |   |_baz. BAN
    |   |_baz2. BAN
    |
    |_foo2. SIMPLE
      |_bar2. SIMPLE
        |_baz. BAN
     */
    @Test
    public void addBanMetricWithSimpleParentsTest() {
        BannedMetricCache bannedMetricCache = new BannedMetricCache();

        bannedMetricCache.addMetricWithStatus("foo.bar.baz.", MetricStatus.BAN);
        bannedMetricCache.addMetricWithStatus("foo.bar.baz2.", MetricStatus.BAN);
        bannedMetricCache.addMetricWithStatus("foo2.bar2.baz.", MetricStatus.BAN);

        Assert.assertEquals(3, bannedMetricCache.getCacheState().bannedMetricsCount);
        Assert.assertEquals(0, bannedMetricCache.getCacheState().autoBannedMetricsCount);
        Assert.assertEquals(7, bannedMetricCache.getCacheState().nodesCount);
    }

    @Test
    public void removeSingleBanMetricTest() {
        BannedMetricCache bannedMetricCache = new BannedMetricCache();

        bannedMetricCache.addMetricWithStatus("foo.", MetricStatus.BAN);
        bannedMetricCache.resetBanStatus("foo.", MetricStatus.SIMPLE);

        Assert.assertEquals(0, bannedMetricCache.getCacheState().bannedMetricsCount);
        Assert.assertEquals(0, bannedMetricCache.getCacheState().autoBannedMetricsCount);
        Assert.assertEquals(0, bannedMetricCache.getCacheState().nodesCount);
    }

    @Test
    public void scipRemoveSingleAutoBanMetricTest() {
        BannedMetricCache bannedMetricCache = new BannedMetricCache();

        bannedMetricCache.addMetricWithStatus("foo.", MetricStatus.AUTO_BAN);
        bannedMetricCache.resetBanStatus("foo.", MetricStatus.SIMPLE);

        Assert.assertEquals(0, bannedMetricCache.getCacheState().bannedMetricsCount);
        Assert.assertEquals(1, bannedMetricCache.getCacheState().autoBannedMetricsCount);
        Assert.assertEquals(1, bannedMetricCache.getCacheState().nodesCount);
    }

    /*
    root                        root
    |_foo. BAN                  |_foo. SIMPLE
      |_bar. SIMPLE   ------>     |_bar. SIMPLE
      | |_baz. BAN                  |_baz. BAN
      |
      |_bar2. SIMPLE
        |_baz. BAN
     */
    @Test
    public void removeBranchBanMetricTest() {
        BannedMetricCache bannedMetricCache = new BannedMetricCache();

        bannedMetricCache.addMetricWithStatus("foo.", MetricStatus.BAN);
        bannedMetricCache.addMetricWithStatus("foo.bar.baz.", MetricStatus.BAN);
        bannedMetricCache.addMetricWithStatus("foo.bar2.baz.", MetricStatus.BAN);
        Assert.assertEquals(3, bannedMetricCache.getCacheState().bannedMetricsCount);
        Assert.assertEquals(5, bannedMetricCache.getCacheState().nodesCount);

        bannedMetricCache.resetBanStatus("foo.", MetricStatus.APPROVED);
        Assert.assertEquals(2, bannedMetricCache.getCacheState().bannedMetricsCount);
        Assert.assertEquals(5, bannedMetricCache.getCacheState().nodesCount);

        bannedMetricCache.resetBanStatus("foo.bar2.baz.", MetricStatus.APPROVED);
        Assert.assertEquals(1, bannedMetricCache.getCacheState().bannedMetricsCount);
        Assert.assertEquals(0, bannedMetricCache.getCacheState().autoBannedMetricsCount);
        Assert.assertEquals(3, bannedMetricCache.getCacheState().nodesCount);
    }

    @Test
    public void resetAutoBanStatusTest() {
        BannedMetricCache bannedMetricCache = new BannedMetricCache();

        bannedMetricCache.addMetricWithStatus("foo.", MetricStatus.AUTO_BAN);
        bannedMetricCache.resetBanStatus("foo.bar.", MetricStatus.HIDDEN);

        Assert.assertEquals(0, bannedMetricCache.getCacheState().bannedMetricsCount);
        Assert.assertEquals(0, bannedMetricCache.getCacheState().autoBannedMetricsCount);
        Assert.assertEquals(0, bannedMetricCache.getCacheState().nodesCount);
    }

    /*
    root
    |_foo. SIMPLE
    | |_bar. SIMPLE
    |   |_baz. BAN
    |   |_baz2. AUTO_BAN
    |
    |_foo2. BAN
      |_bar2. SIMPLE
        |_baz. BAN
     */
    @Test
    public void isBannedMetricTest() {
        BannedMetricCache bannedMetricCache = new BannedMetricCache();

        bannedMetricCache.addMetricWithStatus("foo.bar.baz.", MetricStatus.BAN);
        bannedMetricCache.addMetricWithStatus("foo.bar.baz2.", MetricStatus.AUTO_BAN);
        bannedMetricCache.addMetricWithStatus("foo2.", MetricStatus.BAN);
        bannedMetricCache.addMetricWithStatus("foo2.bar2.baz.", MetricStatus.BAN);
        Assert.assertEquals(3, bannedMetricCache.getCacheState().bannedMetricsCount);
        Assert.assertEquals(1, bannedMetricCache.getCacheState().autoBannedMetricsCount);
        Assert.assertEquals(7, bannedMetricCache.getCacheState().nodesCount);

        Assert.assertFalse(bannedMetricCache.isBanned("foo."));
        Assert.assertFalse(bannedMetricCache.isBanned("foo.bar."));
        Assert.assertTrue(bannedMetricCache.isBanned("foo.bar.baz."));
        Assert.assertTrue(bannedMetricCache.isBanned("foo.bar.baz2."));
        Assert.assertTrue(bannedMetricCache.isBanned("foo2."));
        Assert.assertTrue(bannedMetricCache.isBanned("foo2.bar2."));
        Assert.assertTrue(bannedMetricCache.isBanned("foo2.bar2.baz"));
    }
}

package ru.yandex.market.graphouse.save.tree;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import ru.yandex.market.graphouse.save.UpdateMetricQueueService;
import ru.yandex.market.graphouse.save.banned.BannedMetricCache;
import ru.yandex.market.graphouse.search.MetricStatus;

public class OnRecordMetricTreeTest {

    @Test
    public void getOrCreateMetricTest() {
        OnRecordMetricTree onRecordMetricTree = createOnRecordMetricTree(-1, -1);

        onRecordMetricTree.getOrCreateMetric("foo.bar");

        Assert.assertNotNull(onRecordMetricTree.tryToFindMetric("foo."));
        Assert.assertNotNull(onRecordMetricTree.tryToFindMetric("foo.bar"));
    }

    /*
    root                      root
    |_foo.                    |_foo.
      |_bar.                  | |_bar.
      | |_baz.    ------>     |   |_baz.
      |   |_0_50              |     |_0_50
      |   |_0_99              |
      |                       |_bar2.
      |_bar2.
        |_baz.
          |_0_50
          |_0_99
     */
    @Test
    public void removeMetricFromTreeTest() {
        OnRecordMetricTree onRecordMetricTree = createOnRecordMetricTree(-1, -1);

        onRecordMetricTree.getOrCreateMetric("foo.bar.baz.0_50");
        onRecordMetricTree.getOrCreateMetric("foo.bar.baz.0_99");
        onRecordMetricTree.getOrCreateMetric("foo.bar2.baz.0_50");
        onRecordMetricTree.getOrCreateMetric("foo.bar2.baz.0_99");

        onRecordMetricTree.removeMetricFromTree("foo.bar.baz.0_99");
        onRecordMetricTree.removeMetricFromTree("foo.bar2.baz.");

        Assert.assertNotNull(onRecordMetricTree.tryToFindMetric("foo."));
        Assert.assertNotNull(onRecordMetricTree.tryToFindMetric("foo.bar."));
        Assert.assertNotNull(onRecordMetricTree.tryToFindMetric("foo.bar.baz."));
        Assert.assertNotNull(onRecordMetricTree.tryToFindMetric("foo.bar.baz.0_50"));
        Assert.assertNull(onRecordMetricTree.tryToFindMetric("foo.bar.baz.0_99"));
        Assert.assertNotNull(onRecordMetricTree.tryToFindMetric("foo.bar2."));
        Assert.assertNull(onRecordMetricTree.tryToFindMetric("foo.bar2.baz."));
        Assert.assertNull(onRecordMetricTree.tryToFindMetric("foo.bar2.baz.0_50"));
        Assert.assertNull(onRecordMetricTree.tryToFindMetric("foo.bar2.baz.0_99"));
    }

    @Test
    public void testMetricsLimit() {
        int dirLimit = 5;
        int metricLimit = 10;
        OnRecordMetricTree tree = createOnRecordMetricTree(dirLimit, metricLimit);

        for (int i = 0; i <= dirLimit * 2; i++) {
            Assert.assertEquals(i < dirLimit, tree.getOrCreateMetric("dir.subdir" + i + ".") != null);
        }
        tree.updateMetricIfLoaded("dir.approved-dir.", MetricStatus.APPROVED);

        for (int i = 0; i <= metricLimit * 2; i++) {
            Assert.assertEquals(i < metricLimit, tree.getOrCreateMetric("dir.metric" + i) != null);
        }

        tree.updateMetricIfLoaded("dir.approved-metric", MetricStatus.APPROVED);

        OnRecordMetricDescription dir = tree.getOrCreateMetric("dir.");

        Assert.assertEquals(6, dir.getContent().getContentCount(true));
        Assert.assertEquals(11, dir.getContent().getContentCount(false));

        Assert.assertNull(tree.getOrCreateMetric("dir.one-more-subdir.a.b.c"));
        tree.updateMetricIfLoaded("dir.one-more-subdir.a.b.c", MetricStatus.APPROVED);

        //Run once more to check that already added metric works
        for (int i = 0; i <= dirLimit * 2; i++) {
            Assert.assertEquals(i < dirLimit, tree.getOrCreateMetric("dir.subdir" + i + ".") != null);
        }
        for (int i = 0; i <= metricLimit * 2; i++) {
            Assert.assertEquals(i < metricLimit, tree.getOrCreateMetric("dir.metric" + i) != null);
        }
    }

    private OnRecordMetricTree createOnRecordMetricTree(int maxSubDirPerDir, int maxMetricsPerDir) {
        return new OnRecordMetricTree(
            Mockito.mock(UpdateMetricQueueService.class),
            new BannedMetricCache(),
            Caffeine.newBuilder()
                .recordStats()
                .buildAsync(n -> OnReadDirContent.createEmpty()),
            maxSubDirPerDir,
            maxMetricsPerDir
        );
    }
}

package ru.yandex.market.graphouse.search.tree;

import org.eclipse.jetty.util.ConcurrentHashSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.retention.RetentionProvider;
import ru.yandex.market.graphouse.save.OnRecordCacheUpdater;
import ru.yandex.market.graphouse.save.UpdateMetricQueueService;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.statistics.LoadedMetricsCounter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class DirContentBatcherTest {
    public static long DELAY_BEFORE_ADD_NEW_DIR_MS = 1000;

    private MetricDir root;
    private Semaphore requestSemaphore;
    private Map<String, MetricDir> metrics;

    @Before
    public void init() {
        root = new InMemoryMetricDir(null, "ROOT", MetricStatus.SIMPLE);
        metrics = new HashMap<>();
        metrics.put("ROOT", root);

        requestSemaphore = new Semaphore(0, true);
    }

    @Test
    public void singleBatchTest() throws Exception {
        MetricSearch metricSearch = mockMetricSearch();
        DirContentBatcher dirContentBatcher = new DirContentBatcher(
            metricSearch, 1, 50, 1000, requestSemaphore
        );

        List<Future<DirContent>> futureDirContents = loadBatches(dirContentBatcher, 20, 1);
        requestSemaphore.release();

        for (Future<DirContent> d : futureDirContents) {
            d.get();
        }
        Mockito.verify(metricSearch, Mockito.times(1)).loadDirsContent(Mockito.anySet());
    }

    private MetricSearch mockMetricSearch() {
        MetricSearch metricSearch = Mockito.mock(MetricSearch.class);
        Mockito.when(metricSearch.loadDirsContent(Mockito.anySet())).then(i -> {
            Set<MetricDir> dirs = i.getArgument(0);
            Map<MetricDir, DirContent> result = new HashMap<>();
            dirs.forEach(d -> result.put(d, DirContent.createEmpty()));
            System.out.println("Loaded metrics for " + dirs.size() + " dirs: " + dirs);
            return result;
        });
        return metricSearch;
    }

    @Test(timeout = 5000)
    public void addDirAfterStartExecutionBatchTest() throws Exception {
        loadDirWithLazyQueryExecution(DELAY_BEFORE_ADD_NEW_DIR_MS + 500);
    }

    @Test(timeout = 5000)
    public void addDirAfterFinishExecutionBatchTest() throws ExecutionException, InterruptedException {
        loadDirWithLazyQueryExecution(DELAY_BEFORE_ADD_NEW_DIR_MS - 500);
    }

    private void loadDirWithLazyQueryExecution(long queryExecutionMs) throws ExecutionException, InterruptedException {
        MetricSearch metricSearch = createMetricSearchWithLazyQueryExecution(queryExecutionMs);
        SlowDirContentBatcher dirContentBatcher = new SlowDirContentBatcher(
            metricSearch, 2, 50, 100, requestSemaphore
        );

        List<Future<DirContent>> futureDirContents = loadBatches(dirContentBatcher, 2, 1);
        requestSemaphore.release();

        for (Future<DirContent> d : futureDirContents) {
            d.get();
        }
        Assert.assertFalse(dirContentBatcher.hasAnyErrorsInBatches());
    }

    private MetricSearch createMetricSearchWithLazyQueryExecution(long queryExecutionMs) {
        JdbcTemplate clickHouseJdbcTemplate = Mockito.mock(JdbcTemplate.class);
        Mockito.doAnswer(i -> {
            doSleep(queryExecutionMs);
            return null;
        })
            .when(clickHouseJdbcTemplate)
            .query(Mockito.anyString(), Mockito.any(RowCallbackHandler.class), Mockito.any());
        return new MetricSearch(
            clickHouseJdbcTemplate,
            Mockito.mock(UpdateMetricQueueService.class),
            Mockito.mock(Monitoring.class),
            Mockito.mock(Monitoring.class),
            Mockito.mock(MetricValidator.class),
            Mockito.mock(RetentionProvider.class),
            Mockito.mock(LoadedMetricsCounter.class),
            Mockito.mock(OnRecordCacheUpdater.class),
            "metrics",
            null
        );
    }

    private List<Future<DirContent>> loadBatches(DirContentBatcher dirContentBatcher, int countBatches, long sleepMs) {
        ExecutorService executor = Executors.newFixedThreadPool(countBatches);
        List<Future<DirContent>> futureDirContents = new ArrayList<>();
        for (int i = 0; i < countBatches; i++) {
            String subDir = i + ".";
            futureDirContents.add(
                executor.submit(() -> safeLoadDirContent(
                    dirContentBatcher,
                    createLoadableMetricDir("one_min.test.nginx." + subDir))
                )
            );
            doSleep(sleepMs);
        }
        return futureDirContents;
    }

    private DirContent safeLoadDirContent(DirContentBatcher dirContentBatcher, MetricDir testDir) {
        try {
            return dirContentBatcher.loadDirContent(testDir);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    private MetricDir createLoadableMetricDir(String fullPath) {
        String[] patches = fullPath.split("\\.");
        MetricDir lastDir = root;
        StringBuilder lastDirName = new StringBuilder(root.getName());
        for (String path : patches) {
            if (path.isEmpty()) {
                throw new RuntimeException();
            }
            lastDirName.append(".").append(path);
            MetricDir currentDir = metrics.get(lastDirName.toString());
            if (currentDir == null) {
                currentDir = new InMemoryMetricDir(lastDir, path, MetricStatus.SIMPLE);
                metrics.put(lastDirName.toString(), currentDir);
            }
            lastDir = currentDir;
        }
        return lastDir;
    }

    private static void doSleep(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class SlowDirContentBatcher extends DirContentBatcher {
        private Collection<Batch> allBatches = new ConcurrentHashSet<>();

        public SlowDirContentBatcher(
            MetricSearch metricSearch,
            int maxParallelRequests,
            int maxBatchSize,
            int batchAggregationTimeMillis,
            Semaphore requestSemaphore
        ) {
            super(metricSearch, maxParallelRequests, maxBatchSize, batchAggregationTimeMillis, requestSemaphore);
        }

        @Override
        Batch createNewBatchIfNeed(Batch batch) {
            Batch newBatch = super.createNewBatchIfNeed(batch);
            if (batch != newBatch) {
                allBatches.add(newBatch);
            } else {
                doSleep(DELAY_BEFORE_ADD_NEW_DIR_MS);
            }
            return newBatch;
        }

        public boolean hasAnyErrorsInBatches() {
            return allBatches.stream()
                .filter(batch -> batch.executionStarted)
                .flatMap(batch -> batch.requests.values().stream())
                .anyMatch(future -> {
                    try {
                        future.get(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        return true;
                    }
                    return false;
                });
        }
    }
}

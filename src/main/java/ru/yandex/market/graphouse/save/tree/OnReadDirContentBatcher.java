package ru.yandex.market.graphouse.save.tree;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.market.graphouse.MetricUtil;
import ru.yandex.market.graphouse.MetricValidator;
import ru.yandex.market.graphouse.data.BatchAtomicReference;
import ru.yandex.market.graphouse.data.ClickHouseDirContentLoader;
import ru.yandex.market.graphouse.save.banned.BannedMetricCache;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.statistics.LoadedMetricsCounter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OnReadDirContentBatcher {

    private static final Logger log = LogManager.getLogger();

    private final MetricValidator metricValidator;
    private final BannedMetricCache metricCache;

    private final ClickHouseDirContentLoader clickHouseDirContentLoader;

    private final int maxSubDirsPerDir;
    private final int maxMetricsPerDir;
    private final AsyncLoadingCache<OnRecordMetricDescription, OnReadDirContent> dirContentProvider;

    private final int maxBatchSize;
    private final int batchAggregationTimeMillis;

    private final BatchAtomicReference<Batch> currentBatch = new BatchAtomicReference<>();
    private final Semaphore requestSemaphore;
    private final Semaphore batchSemaphore;

    private final ScheduledExecutorService executorService;

    public OnReadDirContentBatcher(
        MetricValidator metricValidator,
        BannedMetricCache metricCache,
        LoadedMetricsCounter loadedMetricsCounter,
        JdbcTemplate clickHouseJdbcTemplate,
        int queryRetryCount,
        int queryRetryIncrementSec,
        int maxParallelRequests,
        int maxMaxBatchesCount,
        int maxBatchSize,
        String metricsTable,
        int maxSubDirsPerDir,
        int maxMetricsPerDir,
        AsyncLoadingCache<OnRecordMetricDescription, OnReadDirContent> dirContentProvider,
        int batchAggregationTimeMillis
    ) {
        this.metricValidator = metricValidator;
        this.metricCache = metricCache;
        this.clickHouseDirContentLoader = new ClickHouseDirContentLoader(
            clickHouseJdbcTemplate,
            queryRetryCount,
            queryRetryIncrementSec,
            metricsTable,
            loadedMetricsCounter
        );
        this.maxBatchSize = maxBatchSize;
        this.batchAggregationTimeMillis = batchAggregationTimeMillis;
        this.executorService = Executors.newScheduledThreadPool(maxParallelRequests);
        this.batchSemaphore = new Semaphore(maxMaxBatchesCount);
        this.maxSubDirsPerDir = maxSubDirsPerDir;
        this.maxMetricsPerDir = maxMetricsPerDir;
        this.dirContentProvider = dirContentProvider;
        this.requestSemaphore = new Semaphore(maxParallelRequests, true);
    }

    public Future<OnReadDirContent> lazyLoadDirContent(
        OnRecordMetricDescription dir, OnReadDirContent newDirContent
    ) {
        Batch dirBatch = updateAndGetCurrentBatch(dir, newDirContent);
        if (dirBatch == null) {
            return null;
        }
        return dirBatch.getFutureResult(dir);
    }

    private Batch updateAndGetCurrentBatch(
        OnRecordMetricDescription dir, OnReadDirContent newDirContent
    ) {
        try {
            if (!batchSemaphore.tryAcquire(100L, TimeUnit.MILLISECONDS)) {
                return null;
            }
        } catch (InterruptedException e) {
            return null;
        }

        Batch updatedBatch = currentBatch.updateAndGetBatch(
            this::createNewBatchIfNeed,
            batch -> {
                batch.lockForNewRequest();
                return batch.addToBatch(dir, newDirContent);
            },
            Batch::unlockForNewRequest,
            this::scheduleNewBatch
        );
        updatedBatch.unlockForNewRequest();
        return updatedBatch;
    }

    @VisibleForTesting
    Batch createNewBatchIfNeed(Batch batch) {
        if (batch == null || batch.size() >= maxBatchSize) {
            return new Batch();
        }
        return batch;
    }


    private void scheduleNewBatch(Batch batch, Boolean newBatchCreated) {
        if (newBatchCreated) {
            executorService.schedule(batch, batchAggregationTimeMillis, TimeUnit.MILLISECONDS);
        } else {
            batchSemaphore.release();
        }
    }

    public Map<OnRecordMetricDescription, OnReadDirContent> loadDirsContent(
        Set<OnRecordMetricDescription> dirs,
        Map<OnRecordMetricDescription, OnReadDirContent> contentsToLoad
    ) {
        DirContentRequestRowCallbackHandler metricHandler = new DirContentRequestRowCallbackHandler(dirs, contentsToLoad);
        clickHouseDirContentLoader.loadDirsContent(dirs, metricHandler);
        return metricHandler.getResult();
    }

    private class DirContentRequestRowCallbackHandler implements ClickHouseDirContentLoader.DirContentRowCallbackHandler {

        private final Map<String, OnRecordMetricDescription> dirNames;
        private final Map<OnRecordMetricDescription, OnReadDirContent> contentsToLoad;
        private final Map<OnRecordMetricDescription, OnReadDirContent> result = new HashMap<>();

        private String currentDirName = null;
        private boolean autoBan = false;
        private OnRecordMetricDescription currentDir;
        private OnReadDirContent currentDirContent;
        private int metricCount = 0;
        private int dirCount = 0;


        public DirContentRequestRowCallbackHandler(
            Set<OnRecordMetricDescription> requestDirs,
            Map<OnRecordMetricDescription, OnReadDirContent> contentsToLoad
        ) {
            dirNames = requestDirs.stream().collect(Collectors.toMap(OnRecordMetricDescription::getName, Function.identity()));
            this.contentsToLoad = contentsToLoad;
        }

        @Override
        public void processRow(ResultSet rs) throws SQLException {
            String dirName = rs.getString("parent");
            checkNewDir(dirName);

            if (currentDir == null || currentDirContent == null || autoBan) {
                return;
            }

            String fullName = rs.getString("name");
            if (!metricValidator.validate(fullName, true)) {
                log.warn("Invalid metric in db: " + fullName);
                return;
            }

            MetricStatus status = MetricStatus.valueOf(rs.getString("last_status"));

            boolean isDir = MetricUtil.isDir(fullName);
            String name = MetricUtil.getLastLevelName(fullName).intern();
            int maxContentSize = isDir ? maxSubDirsPerDir : maxMetricsPerDir;

            if (maxContentSize > 0 && currentDirContent.getContentCount(isDir) >= maxContentSize && !status.handmade()) {
                metricCache.addMetricWithStatus(currentDirName, MetricStatus.AUTO_BAN);
                autoBan = true;
                return;
            }

            if (isDir) {
                ++dirCount;
            } else {
                ++metricCount;
            }

            if (MetricStatus.BAN == status) {
                return;
            }

            OnRecordMetricDescription loadedDescription = currentDirContent.computeDescriptionIfAbsent(
                name, isDir,
                n -> new OnRecordMetricDescription(dirContentProvider, currentDir, name, status, isDir)
            );
            loadedDescription.setParent(currentDir);
            loadedDescription.setMaybeNewMetrics(false);
        }

        private void checkNewDir(String dirName) {
            if (currentDirName == null || !currentDirName.equals(dirName)) {
                flushResult();
                currentDirName = dirName;
                currentDir = dirNames.remove(dirName);

                if (currentDir == null) {
                    return;
                }

                currentDirContent = contentsToLoad.get(currentDir);

                if (currentDirContent == null) {
                    currentDirContent = OnReadDirContent.createEmpty();
                }
            }
        }

        private void flushResult() {
            if (currentDirContent != null && currentDir != null) {
                result.put(currentDir, currentDirContent);
                currentDirContent.setLoaded(true);
            }
            currentDirName = null;
            currentDirContent = null;
            autoBan = false;
        }

        public Map<OnRecordMetricDescription, OnReadDirContent> getResult() {
            flushResult();
            for (OnRecordMetricDescription metricDir : dirNames.values()) {
                result.put(metricDir, OnReadDirContent.createEmpty());
            }
            return result;
        }

        @Override
        public int getMetricCount() {
            return metricCount;
        }

        @Override
        public int getDirCount() {
            return dirCount;
        }
    }

    @VisibleForTesting
    class Batch implements Runnable {
        private final ReadWriteLock requestsLock = new ReentrantReadWriteLock();
        volatile boolean executionStarted = false;
        final Map<OnRecordMetricDescription, SettableFuture<OnReadDirContent>> requests = new ConcurrentHashMap<>();
        final Map<OnRecordMetricDescription, OnReadDirContent> contentsToLoad = new ConcurrentHashMap<>();
        private Set<OnRecordMetricDescription> notLoadedMetricDir = null;

        @Override
        public void run() {
            requestSemaphore.acquireUninterruptibly();
            try {
                lockRequestsForExecution();
                executionStarted = true;
                resetCurrentBatch();
                loadDirsContent();
            } catch (Exception e) {
                updateNotLoadedMetrics(e);
            } finally {
                unlockRequestsAfterExecution();
                requestSemaphore.release();
                batchSemaphore.release();
            }
        }

        private void resetCurrentBatch() {
            currentBatch.getAndUpdate(batch -> (batch == this) ? null : batch);
        }

        private void loadDirsContent() {
            notLoadedMetricDir = new HashSet<>(requests.keySet());

            Map<OnRecordMetricDescription, OnReadDirContent> dirsContent = OnReadDirContentBatcher.this.loadDirsContent(requests.keySet(), contentsToLoad);

            for (Map.Entry<OnRecordMetricDescription, OnReadDirContent> dirDirContentEntry : dirsContent.entrySet()) {
                OnRecordMetricDescription metric = dirDirContentEntry.getKey();
                OnReadDirContent loadedContent = dirDirContentEntry.getValue();
                SettableFuture<OnReadDirContent> dirContent = requests.get(metric);
                if (dirContent != null) {
                    OnReadDirContent resultContent = contentsToLoad.get(metric);
                    if (resultContent == null) {
                        resultContent = loadedContent;
                    }
                    dirContent.set(resultContent);
                    notLoadedMetricDir.remove(metric);
                }
            }

            if (!notLoadedMetricDir.isEmpty()) {
                log.error(notLoadedMetricDir.size() + " requests without data for dirs: " + notLoadedMetricDir);
                throw new IllegalStateException("No data for dirs");
            }
        }

        private void updateNotLoadedMetrics(Exception e) {
            log.error("Failed to load content for dirs: " + requests.keySet(), e);

            if (notLoadedMetricDir == null) {
                for (SettableFuture<OnReadDirContent> settableFuture : requests.values()) {
                    settableFuture.setException(e);
                }
            } else {
                for (OnRecordMetricDescription metricDir : notLoadedMetricDir) {
                    requests.get(metricDir).setException(e);
                }
            }
        }

        public boolean addToBatch(OnRecordMetricDescription dir, OnReadDirContent newDirContent) {
            if (executionStarted) {
                return false;
            }
            requests.computeIfAbsent(dir, metricDir -> SettableFuture.create());
            contentsToLoad.put(dir, newDirContent);
            return true;
        }

        public Future<OnReadDirContent> getFutureResult(OnRecordMetricDescription dir) {
            Future<OnReadDirContent> future = requests.get(dir);
            Preconditions.checkNotNull(future);
            return future;
        }

        public int size() {
            return requests.size();
        }

        public void lockForNewRequest() {
            requestsLock.readLock().lock();
        }

        public void unlockForNewRequest() {
            requestsLock.readLock().unlock();
        }

        private void lockRequestsForExecution() {
            requestsLock.writeLock().lock();
        }

        private void unlockRequestsAfterExecution() {
            requestsLock.writeLock().unlock();
        }
    }
}

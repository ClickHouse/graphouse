package ru.yandex.market.graphouse.search.tree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.market.graphouse.data.BatchAtomicReference;
import ru.yandex.market.graphouse.search.MetricSearch;

import java.util.Collections;
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

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 31/01/2017
 */
public class DirContentBatcher {

    private static final Logger log = LogManager.getLogger();

    private final MetricSearch metricSearch;

    private final int maxBatchSize;
    private final int batchAggregationTimeMillis;

    private final BatchAtomicReference<Batch> currentBatch = new BatchAtomicReference<>();
    private final Semaphore requestSemaphore;

    private final ScheduledExecutorService executorService;


    public DirContentBatcher(MetricSearch metricSearch, int maxParallelRequests,
                             int maxBatchSize, int batchAggregationTimeMillis) {
        this(
            metricSearch, maxParallelRequests, maxBatchSize, batchAggregationTimeMillis,
            new Semaphore(maxParallelRequests, true)
        );
    }

    @VisibleForTesting
    DirContentBatcher(MetricSearch metricSearch, int maxParallelRequests,
                      int maxBatchSize, int batchAggregationTimeMillis, Semaphore requestSemaphore) {
        this.metricSearch = metricSearch;
        this.maxBatchSize = maxBatchSize;
        this.batchAggregationTimeMillis = batchAggregationTimeMillis;
        this.executorService = Executors.newScheduledThreadPool(maxParallelRequests);
        this.requestSemaphore = requestSemaphore;
    }


    public DirContent loadDirContent(MetricDir dir) throws Exception {

        /*
         * If we have available permit we run immediately, otherwise create pending batch.
         */

        if (requestSemaphore.tryAcquire()) {
            try {
                return metricSearch.loadDirsContent(Collections.singleton(dir)).get(dir);
            } finally {
                requestSemaphore.release();
            }
        }

        Batch dirBatch = updateAndGetCurrentBatch(dir);
        return dirBatch.getResult(dir);
    }

    private Batch updateAndGetCurrentBatch(MetricDir dir) {
        Batch updatedBatch = currentBatch.updateAndGetBatch(
            this::createNewBatchIfNeed,
            batch -> {
                batch.lockForNewRequest();
                return batch.addToBatch(dir);
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


    private void scheduleNewBatch(Batch batch, boolean newBatch) {
        if (newBatch) {
            executorService.schedule(batch, batchAggregationTimeMillis, TimeUnit.MILLISECONDS);
        }
    }

    @VisibleForTesting
    class Batch implements Runnable {
        private final ReadWriteLock requestsLock = new ReentrantReadWriteLock();
        volatile boolean executionStarted = false;
        final Map<MetricDir, SettableFuture<DirContent>> requests = new ConcurrentHashMap<>();
        private Set<MetricDir> notLoadedMetricDir = null;

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
            }
        }

        private void resetCurrentBatch() {
            currentBatch.getAndUpdate(batch -> (batch == this) ? null : batch);
        }

        private void loadDirsContent() {
            notLoadedMetricDir = new HashSet<>(requests.keySet());
            Map<MetricDir, DirContent> dirsContent = metricSearch.loadDirsContent(requests.keySet());

            for (Map.Entry<MetricDir, DirContent> dirDirContentEntry : dirsContent.entrySet()) {
                SettableFuture<DirContent> dirContent = requests.get(dirDirContentEntry.getKey());
                if (dirContent != null) {
                    dirContent.set(dirDirContentEntry.getValue());
                    notLoadedMetricDir.remove(dirDirContentEntry.getKey());
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
                for (SettableFuture<DirContent> settableFuture : requests.values()) {
                    settableFuture.setException(e);
                }
            } else {
                for (MetricDir metricDir : notLoadedMetricDir) {
                    requests.get(metricDir).setException(e);
                }
            }
        }

        public boolean addToBatch(MetricDir dir) {
            if (executionStarted) {
                return false;
            }
            requests.computeIfAbsent(dir, metricDir -> SettableFuture.create());
            return true;
        }

        public DirContent getResult(MetricDir dir) throws Exception {
            Future<DirContent> future = requests.get(dir);
            Preconditions.checkNotNull(future);
            return future.get();
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

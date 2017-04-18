package ru.yandex.market.graphouse.perf;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 10/04/2017
 */
public class InsertTest extends AbstractScheduledService {

    private static final Logger log = LogManager.getLogger();

    private final PerfArgs args;

    private final int sendIntervalMillis;

    private final ScheduledExecutorService executorService;

    private final AtomicLong totalSendsMetrics = new AtomicLong();
    private final AtomicLong totalFailedMetrics = new AtomicLong();

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(sendIntervalMillis, sendIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private static class PerfArgs {

        @Parameter(names = "--host")
        private String host = "localhost";

        @Parameter(names = "--port")
        private Integer port = 2003;

        @Parameter(names = "--prefix", description = "Prefix for metrics")
        private String metricPrefix = "graphouse.perf-test.";

        @Parameter(names = "--threads", description = "Threads count")
        private Integer threadCount = 100;

        @Parameter(names = "--metrics", description = "Metrics per thread")
        private Integer metricPerThread = 1000;

        @Parameter(names = "--interval", description = "Send interval in seconds")
        private Integer sendIntervalSeconds = 10;

        @Parameter(names = "--timeout", description = "Send timeout in seconds")
        private Integer sendTimeoutSeconds = 30;

        @Parameter(names = {"-h", "--help"}, help = true)
        private boolean help;
    }

    public static void main(String[] args) throws InterruptedException {
        PerfArgs perfArgs = new PerfArgs();
        JCommander jCommander = new JCommander(perfArgs, args);
        if (perfArgs.help) {
            jCommander.usage();
            System.exit(0);
        }
        if (!perfArgs.metricPrefix.endsWith(".")) {
            perfArgs.metricPrefix += ".";
        }
        InsertTest insertTest = new InsertTest(perfArgs);
        insertTest.startAsync().awaitRunning();
    }

    public InsertTest(PerfArgs args) {
        this.args = args;
        sendIntervalMillis = (int) TimeUnit.SECONDS.toMillis(args.sendIntervalSeconds);

        executorService = Executors.newScheduledThreadPool(args.threadCount);
        log.info("Creating graphite insert perf test for " + args.host + ":" + args.port);
        double metricsPerSeconds = (args.threadCount * args.metricPerThread) / args.sendIntervalSeconds;
        log.info(
            "Thread count: " + args.threadCount + ", metric per thread: " + args.metricPerThread +
                ", send interval: " + TimeUnit.MILLISECONDS.toSeconds(sendIntervalMillis) + " seconds. " +
                "Metrics per second: " + metricsPerSeconds + "."
        );
    }

    @Override
    protected void runOneIteration() throws Exception {
        int timestampSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        Counter counter = new Counter(timestampSeconds);
        for (int i = 1; i <= args.threadCount; i++) {
            String prefix = args.metricPrefix + "thread" + i + ".";
            SendWorker worker = new SendWorker(prefix, timestampSeconds, args.metricPerThread, counter);
            int delayMillis = ThreadLocalRandom.current().nextInt(sendIntervalMillis);
            executorService.schedule(worker, delayMillis, TimeUnit.MILLISECONDS);
        }
        executorService.execute(counter);
    }

    private Socket createSocket() throws IOException {
        Socket socket = new Socket(args.host, args.port);
        socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(args.sendTimeoutSeconds));
        socket.setTcpNoDelay(true);
        return socket;
    }

    private class Counter implements Runnable {

        private final int timestampSeconds;
        private final CountDownLatch latch;
        private final AtomicInteger sendMetrics = new AtomicInteger();
        private final AtomicInteger failedToSend = new AtomicInteger();
        private final AtomicLong timeNanos = new AtomicLong();
        private volatile boolean actual = true;

        public Counter(int timestampSeconds) {
            latch = new CountDownLatch(args.threadCount);
            this.timestampSeconds = timestampSeconds;
        }

        @Override
        public void run() {
            try {
                latch.await(args.sendIntervalSeconds + args.sendTimeoutSeconds * 2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
            actual = false;
            int unknownThreadsCount = (int) latch.getCount();
            int unknownMetricsCount = args.metricPerThread * unknownThreadsCount;
            long totalSend = totalSendsMetrics.get();
            long totalFailed = totalFailedMetrics.addAndGet(unknownMetricsCount);
            failedToSend.addAndGet(unknownMetricsCount);

            Date date = new Date(timestampSeconds * 1000L);
            double errorPercent = failedToSend.get() * 100.0 / (failedToSend.get() + sendMetrics.get());
            double totalErrorPercent = totalFailed * 100.0 / (totalSend + totalFailed);
            long sendTimeMillis = TimeUnit.NANOSECONDS.toMillis(timeNanos.get());
            double sendTimePerThread = (double) sendTimeMillis / (args.threadCount - unknownThreadsCount);
            log.info(
                "Finished sending metrics for timestamp " + timestampSeconds + " (" + date + "). " +
                    "Send " + sendMetrics.get() + " (" + totalSend + " total), " +
                    "failed to send " + failedToSend.get() + " (" + totalFailed + " total), " +
                    "errors " + errorPercent + "% (" + totalErrorPercent + "% total). " +
                    "Total send time " + sendTimeMillis + " ms, avg " + sendTimePerThread + " ms per thread."
            );
            if (unknownThreadsCount > 0) {
                log.warn("Failed to get info for " + unknownThreadsCount + " threads. Timestamp " + timestampSeconds);
            }
        }

        public void onSuccess(int count, long elapsedNanos) {
            if (actual) {
                sendMetrics.addAndGet(count);
                totalSendsMetrics.addAndGet(count);
                latch.countDown();
                timeNanos.addAndGet(elapsedNanos);
            }
        }

        public void onFail(int count) {
            if (actual) {
                failedToSend.addAndGet(count);
                totalFailedMetrics.addAndGet(count);
                latch.countDown();
            }
        }

        public boolean isOutdated() {
            return !actual;
        }
    }

    private class SendWorker implements Runnable {

        private final String prefix;
        private final int timestamp;
        private final int count;
        private final Counter counter;

        public SendWorker(String prefix, int timestamp, int count, Counter counter) {
            this.prefix = prefix;
            this.timestamp = timestamp;
            this.count = count;
            this.counter = counter;
        }

        @Override
        public void run() {
            if (counter.isOutdated()) {
                log.info("Thread for timestamp " + toString() + " is outdated, not running");
                return;
            }
            Stopwatch stopwatch = Stopwatch.createStarted();
            try (Socket socket = createSocket()) {
                try (BufferedOutputStream outputStream = new BufferedOutputStream(socket.getOutputStream())) {
                    for (int i = 1; i <= count; i++) {
                        if (counter.isOutdated()) {
                            log.info(
                                "Stopping metric send for timestamp " + timestamp + " cause outdated. " +
                                    "Sent " + i + "metrics, " + (count - i) + " left."
                            );
                            return;
                        }
                        double value = ThreadLocalRandom.current().nextDouble(1000);
                        String line = prefix + "metric" + i + " " + value + " " + timestamp + "\n";
                        outputStream.write(line.getBytes());
                    }
                }
                stopwatch.stop();
                counter.onSuccess(count, stopwatch.elapsed(TimeUnit.NANOSECONDS));
            } catch (Exception e) {
                log.warn("Failed to send " + count + " metrics " + e.getMessage());
                counter.onFail(count);
            }
        }
    }

}

package ru.yandex.market.graphouse.perf;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 10/04/2017
 */
public class InsertTest extends AbstractScheduledService {

    private static final Logger log = LogManager.getLogger();

    private final PerfArgs args;

    private final int sendIntervalMillis;

    private final ScheduledExecutorService executorService;

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
        for (int i = 1; i <= args.threadCount; i++) {
            String prefix = args.metricPrefix + "thread" + i + ".";
            SendWorker worker = new SendWorker(prefix, timestampSeconds, args.metricPerThread);
            int delayMillis = ThreadLocalRandom.current().nextInt(sendIntervalMillis);
            executorService.schedule(worker, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    private Socket createSocket() throws IOException {
        return new Socket(args.host, args.port);
    }

    private class SendWorker implements Runnable {

        private final String prefix;
        private final int timestamp;
        private final int count;

            public SendWorker(String prefix, int timestamp, int count) {
            this.prefix = prefix;
            this.timestamp = timestamp;
            this.count = count;
        }

        @Override
        public void run() {
            try (Socket socket = createSocket()) {
                try (PrintWriter writer = new PrintWriter(socket.getOutputStream())) {
                    for (int i = 1; i <= count; i++) {
                        double value = ThreadLocalRandom.current().nextDouble(1000);
                        String line = prefix + "metric" + i + " " + value + " " + timestamp;
                        writer.println(line);
                    }
                }
            } catch (IOException e) {
                log.error("Exception", e);
            }
        }
    }

}

package ru.yandex.market.graphouse.server;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import ru.yandex.market.graphouse.cacher.MetricCacher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 02/04/15
 */
public class MetricServer implements InitializingBean {

    private static final Logger log = LogManager.getLogger();

    @Value("${graphouse.cacher.port}")
    private int port;

    @Value("${graphouse.cacher.bind-address}")
    private String bindAddress;

    @Value("${graphouse.cacher.socket-timeout-millis}")
    private int socketTimeoutMillis;

    @Value("${graphouse.cacher.threads}")
    private int threadCount;

    @Value("${graphouse.cacher.read-batch-size}")
    private int readBatchSize;

    @Value("${graphouse.log.remote-socket-address:false}")
    private boolean shouldLogRemoteSocketAddress;

    private ServerSocket serverSocket;
    private ExecutorService readersExecutorService;
    private ExecutorService writersExecutorService;

    private final MetricCacher metricCacher;
    private final MetricFactory metricFactory;

    public MetricServer(MetricCacher metricCacher, MetricFactory metricFactory) {
        this.metricCacher = metricCacher;
        this.metricFactory = metricFactory;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("Starting metric server on port: " + port);
        serverSocket = new ServerSocket();
        SocketAddress socketAddress;

        if (Strings.isNullOrEmpty(bindAddress)) {
            socketAddress = new InetSocketAddress(port);
        } else {
            socketAddress = new InetSocketAddress(bindAddress, port);
        }
        serverSocket.bind(socketAddress);

        log.info("Starting " + threadCount + " metric server threads");

        readersExecutorService = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            readersExecutorService.submit(new MetricServerWorker());
        }

        writersExecutorService = new ThreadPoolExecutor(
            threadCount,
            threadCount,
            1, TimeUnit.SECONDS,
            // Queue is limited so that we won't eat all available memory if we are reading metrics from sockets faster
            // than we are able to write them to ClickHouse.
            new ArrayBlockingQueue<>(10 * threadCount),
            // Whenever a task is rejected because the task queue is full we will run it in the thread that attempted to
            // enqueue it. This ensures that (1) we won't just ignore rejected tasks and (2) we will stop reading new
            // metrics from sockets when the queue is full.
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Shutting down metric server");
                readersExecutorService.shutdownNow();
                writersExecutorService.shutdownNow();
                try {
                    serverSocket.close();
                } catch (IOException ignored) {
                }
                log.info("Metric server stopped");
            }
        }));
        log.info("Metric server started on port " + port);
    }

    private class MetricServerWorker implements Runnable {

        private List<UnparsedLine> metrics = new ArrayList<>(readBatchSize);

        @Override
        public void run() {
            while (!Thread.interrupted() && !serverSocket.isClosed()) {
                try {
                    read();
                } catch (Exception e) {
                    log.warn("Failed to read data", e);
                }
            }
            log.info("MetricServerWorker stopped");
        }

        private void read() throws IOException {
            metrics.clear();
            Socket socket = serverSocket.accept();

            if (shouldLogRemoteSocketAddress) {
                log.info("Connection accepted. Client's address: '{}'", socket.getRemoteSocketAddress().toString());
            }

            try {
                socket.setSoTimeout(socketTimeoutMillis);
                socket.setKeepAlive(false);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    metrics.add(new UnparsedLine(
                        line,
                        (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())
                    ));
                    if (metrics.size() >= readBatchSize) {
                        submitAndClearMetrics();
                    }
                }
            } catch (SocketTimeoutException e) {
                log.warn("Socket timeout from " + socket.getRemoteSocketAddress().toString());
            } finally {
                socket.close();
            }
            submitAndClearMetrics();
        }

        private void submitAndClearMetrics() {
            List<UnparsedLine> metricsToParseAndSave = metrics;
            metrics = new ArrayList<>(readBatchSize);
            writersExecutorService.submit(
                () -> metricCacher.submitMetrics(
                    metricsToParseAndSave.stream()
                        .map(unparsedLine -> metricFactory.createMetric(
                            unparsedLine.line,
                            unparsedLine.updatedSeconds
                        ))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList())
                )
            );
        }
    }

    private static class UnparsedLine {
        final String line;
        final int updatedSeconds;

        private UnparsedLine(String line, int updatedSeconds) {
            this.line = line;
            this.updatedSeconds = updatedSeconds;
        }
    }

    public void setSocketTimeoutMillis(int socketTimeoutMillis) {
        this.socketTimeoutMillis = socketTimeoutMillis;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public void setPort(int port) {
        this.port = port;
    }
}

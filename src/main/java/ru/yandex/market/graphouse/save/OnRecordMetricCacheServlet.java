package ru.yandex.market.graphouse.save;

import ru.yandex.market.graphouse.statistics.AccumulatedMetric;
import ru.yandex.market.graphouse.statistics.StatisticsService;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class OnRecordMetricCacheServlet extends HttpServlet {
    private final OnRecordCacheUpdater onRecordCacheUpdater;
    private final OnRecordMetricProvider onRecordMetricProvider;
    private final StatisticsService statisticsService;

    public OnRecordMetricCacheServlet(
        OnRecordCacheUpdater onRecordCacheUpdater,
        OnRecordMetricProvider onRecordMetricProvider,
        StatisticsService statisticsService
    ) {
        this.onRecordCacheUpdater = onRecordCacheUpdater;
        this.onRecordMetricProvider = onRecordMetricProvider;
        this.statisticsService = statisticsService;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        statisticsService.accumulateMetric(AccumulatedMetric.NUMBER_OF_WEB_REQUESTS, 1);

        if (isServerNotReady()) {
            respondMetricsNotLoaded(resp);
            return;
        }

        if (isOnRecordCacheDisabled()) {
            respondMetricsDisabled(resp);
            return;
        }

        switch (req.getRequestURI()) {
            case "/checkOnRecordCache":
                checkMetricStateInCache(req, resp);
                break;
            case "/printBannedCacheState":
                printBannedCacheState(resp);
                break;
            default:
                badRequest(resp);
                break;
        }
    }

    private boolean isServerNotReady() {
        return !onRecordCacheUpdater.isMetricCacheInitialized();
    }

    private boolean isOnRecordCacheDisabled() {
        return onRecordCacheUpdater.isOnRecordCacheDisabled();
    }

    private void respondMetricsNotLoaded(HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        resp.getWriter().println("Metric tree not loaded\n");
        resp.getWriter().println("Loading status: " + onRecordCacheUpdater.getCacheInitMonitoring().toString());
    }

    private void respondMetricsDisabled(HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        resp.getWriter().println("OnRecordMetricCache is disabled");
    }

    private void checkMetricStateInCache(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String metricName = req.getParameter("metricName");
        final PrintWriter writer = resp.getWriter();

        if (metricName == null || metricName.isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            writer.println(
                "Usage:\n" +
                    "GET /checkOnRecordCache?metricName=<metric_name>\n"
            );
            return;
        }

        onRecordMetricProvider.traceMetricStateInCache(metricName, writer);
    }

    private void printBannedCacheState(HttpServletResponse resp) throws IOException {
        resp.getOutputStream().println(onRecordMetricProvider.printBannedCacheState());
    }

    private void badRequest(HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getOutputStream().println("Usage:");
        resp.getOutputStream().println("GET  /checkOnRecordCache?metricName=<metric_name>");
    }
}

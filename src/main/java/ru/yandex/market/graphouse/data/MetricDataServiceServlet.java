package ru.yandex.market.graphouse.data;

import com.google.common.base.Stopwatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 31.10.16
 */
public class MetricDataServiceServlet extends HttpServlet {

    private static final Logger log = LogManager.getLogger();

    private final MetricDataService metricDataService;
    private final int maxMetricsPerQuery;
    private final int responseBufferSizeBytes;

    public MetricDataServiceServlet(MetricDataService metricDataService, int maxMetricsPerQuery,
                                    int responseBufferSizeBytes) {
        this.metricDataService = metricDataService;
        this.maxMetricsPerQuery = maxMetricsPerQuery;
        this.responseBufferSizeBytes = responseBufferSizeBytes;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        getData(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        getData(req, resp);
    }

    private void getData(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setBufferSize(responseBufferSizeBytes);

        Stopwatch stopwatch = Stopwatch.createStarted();

        final String metricsString = req.getParameter("metrics");

        if (metricsString == null || metricsString.isEmpty()) {
            log.warn("Metrics list is empty");
            writeBadRequest(resp);
            return;
        }

        final List<String> metrics = Arrays.asList(metricsString.split(","));
        if (maxMetricsPerQuery > 0 && metrics.size() > maxMetricsPerQuery) {
            resp.setStatus(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
            resp.getOutputStream().println(String.format(
                "Too many metrics in query. Provided %d, max %d", metrics.size(), maxMetricsPerQuery
            ));
            return;
        }

        final int startTimeSeconds;
        final int endTimeSeconds;

        try {
            startTimeSeconds = Integer.parseInt(req.getParameter("start"));
            endTimeSeconds = Integer.parseInt(req.getParameter("end"));
        } catch (NumberFormatException e) {
            log.warn("Failed to parse timestamp", e);
            writeBadRequest(resp);
            return;
        }

        final String reqKey = req.getParameter("reqKey");


        try {
            metricDataService.getData(metrics, startTimeSeconds, endTimeSeconds, resp.getWriter());
        } catch (Exception e) {
            log.error("Problems with request (" + reqKey + " ): " + req.getRequestURI(), e);
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            e.printStackTrace(resp.getWriter());
            return;
        }
        stopwatch.stop();
        if (log.isDebugEnabled()) {
            log.debug(
                "Metric data request processed in " + stopwatch.toString() +
                    ". Start=" + startTimeSeconds + ", end=" + endTimeSeconds +
                    ", metrics (" + metrics.size() + "): " + metrics.toString()
            );
        }
        resp.setStatus(HttpServletResponse.SC_OK);
    }

    private void writeBadRequest(HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getOutputStream().println("Usage:");
        resp.getOutputStream().println("/metricData?");
        resp.getOutputStream().println("\t metrics=<metric1,metric2...>");
        resp.getOutputStream().println("\t &start=<startTimeSeconds>");
        resp.getOutputStream().println("\t &end=<endTimeSeconds>");
        resp.getOutputStream().println("\t [&reqKey=<reqKey>]");
    }

}

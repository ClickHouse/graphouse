package ru.yandex.market.graphouse.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.market.graphouse.statistics.AccumulatedMetric;
import ru.yandex.market.graphouse.statistics.StatisticsService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 08/04/15
 */
public class MetricSearchServlet extends HttpServlet {

    private static final Logger log = LogManager.getLogger();
    private final MetricSearch metricSearch;
    private final StatisticsService statisticsService;

    public MetricSearchServlet(MetricSearch metricSearch, StatisticsService statisticsService) {
        this.metricSearch = metricSearch;
        this.statisticsService = statisticsService;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        statisticsService.accumulateMetric(AccumulatedMetric.NUMBER_OF_WEB_REQUESTS, 1);

        if (isServerNotReady()) {
            respondMetricsNotLoaded(resp);
            return;
        }

        switch (req.getRequestURI()) {
            case "/search":
                search(req, resp);
                break;
            case "/ban":
                modify(req, resp, MetricStatus.BAN);
                break;
            case "/multiBan":
                multiModify(req, resp, MetricStatus.BAN);
                break;
            case "/approve":
                modify(req, resp, MetricStatus.APPROVED);
                break;
            case "/multiApprove":
                multiModify(req, resp, MetricStatus.APPROVED);
                break;
            case "/hide":
                modify(req, resp, MetricStatus.HIDDEN);
                break;
            case "/multiHide":
                multiModify(req, resp, MetricStatus.HIDDEN);
                break;
            case "/searchCachedMetrics":
                searchCachedMetrics(req, resp);
                break;
            case "/metricTreeState":
                printMetricTreeState(resp);
                break;
            default:
                badRequest(resp);
                break;
        }
    }

    private void respondMetricsNotLoaded(HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        resp.getWriter().println("Metric tree not loaded\n");
        resp.getWriter().println("Loading status: " + metricSearch.getMetricSearchUnit().toString());
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        if (isServerNotReady()) {
            respondMetricsNotLoaded(resp);
            return;
        }

        switch (req.getRequestURI()) {
            case "/search":
                search(req, resp);
                break;
            default:
                badRequest(resp);
                break;
        }
    }

    private boolean isServerNotReady() {
        return !metricSearch.isMetricTreeLoaded();
    }

    private void badRequest(HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getOutputStream().println("Usage:");
        resp.getOutputStream().println("GET  /search?query=<pattern>");
        resp.getOutputStream().println("POST /search (body: query=<url_encoded_search_query>)");
        resp.getOutputStream().println("GET  /ban?name=<metric>");
        resp.getOutputStream().println("GET  /multiBan?query=<pattern>");
        resp.getOutputStream().println("GET  /approve?name=<metric>");
        resp.getOutputStream().println("GET  /multiApprove?query=<pattern>");
        resp.getOutputStream().println("GET  /hide?name=<metric>");
        resp.getOutputStream().println("GET  /multiHide?query=<pattern>");
        resp.getOutputStream().println("GET  /searchCachedMetrics?query=<pattern>");
        resp.getOutputStream().println("GET  /metricTreeState");
    }

    private void modify(HttpServletRequest req, HttpServletResponse resp, MetricStatus status) throws IOException {
        String metric = req.getParameter("name");
        if (metric == null || metric.isEmpty()) {
            badRequest(resp);
            return;
        }
        metricSearch.modify(metric, status);
        resp.getOutputStream().println("Updated to status " + status + ": " + metric);

    }

    private void multiModify(HttpServletRequest req, HttpServletResponse resp, MetricStatus status) throws IOException {
        String query = req.getParameter("query");
        if (query == null || query.isEmpty()) {
            badRequest(resp);
            return;
        }

        final PrintWriter writer = resp.getWriter();
        writer.println("Status changed to " + status.name() + ":");
        writer.println();
        int count = metricSearch.multiModify(query, status, writer);
        writer.println();
        writer.println("Total count: " + count);
    }

    private void search(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String query = req.getParameter("query");
        final PrintWriter writer = resp.getWriter();

        if (query == null || query.isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            writer.println("Usage:\n" +
                "GET /search?query=<search_query>\n" +
                "POST /search (body: search=<url_encoded_search_query>)");
            return;
        }

        metricSearch.search(query, writer);
    }

    private void searchCachedMetrics(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String query = req.getParameter("query");
        final PrintWriter writer = resp.getWriter();

        if (query == null || query.isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            writer.println(
                "Usage:\n" +
                    "GET /searchCachedMetrics?query=<search_query>\n" +
                    "GET /searchCachedMetrics?query=<search_query>&writeLoadedInfo=true\n"
            );
            return;
        }

        String writeLoadedInfoParam = req.getParameter("writeLoadedInfo");
        boolean writeLoadedInfo = false;
        if (writeLoadedInfoParam != null && !writeLoadedInfoParam.isEmpty()) {
            writeLoadedInfo = Boolean.parseBoolean(writeLoadedInfoParam);
        }

        metricSearch.searchCachedMetrics(query, writer, writeLoadedInfo);
    }

    private void printMetricTreeState(HttpServletResponse resp) throws IOException {
        resp.getOutputStream().println(metricSearch.getMetricTreeState());
    }
}

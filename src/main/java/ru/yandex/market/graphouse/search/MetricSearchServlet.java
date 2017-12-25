package ru.yandex.market.graphouse.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.market.graphouse.statistics.AccumulatedMetric;
import ru.yandex.market.graphouse.statistics.IStatisticsService;

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
    private final IStatisticsService statisticsService;
    private final boolean allowColdRun;

    public MetricSearchServlet(MetricSearch metricSearch, IStatisticsService statisticsService, boolean allowColdRun) {
        this.metricSearch = metricSearch;
        this.statisticsService = statisticsService;
        this.allowColdRun = allowColdRun;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        statisticsService.accumulateMetric(AccumulatedMetric.NUMBER_OF_WEB_REQUESTS, 1);

        if (!allowColdRun && !metricSearch.isMetricTreeLoaded()) {
            resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            resp.getWriter().println("Metric tree not loaded\n");
            resp.getWriter().println("Loading status: " + metricSearch.getMetricSearchUnit().toString());
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
            default:
                badRequest(resp);
                break;
        }
    }

    private void badRequest(HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getOutputStream().println("Usage:");
        resp.getOutputStream().println("/search?query=<pattern>");
        resp.getOutputStream().println("/ban?name=<metric>");
        resp.getOutputStream().println("/multiBan?query=<pattern>");
        resp.getOutputStream().println("/approve?name=<metric>");
        resp.getOutputStream().println("/multiApprove?query=<pattern>");
        resp.getOutputStream().println("/hide?name=<metric>");
        resp.getOutputStream().println("/multiHide?query=<pattern>");
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
            writer.println("Usage: /search?query=<searchquery>");
            return;
        }
        metricSearch.search(query, writer);
    }
}

package ru.yandex.market.graphouse.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Resource;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 08/04/15
 */
public class MetricSearchServlet extends HttpServlet {

    private static final Logger log = LogManager.getLogger();
    private MetricSearch metricSearch;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
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

    @Resource
    public void setMetricSearch(MetricSearch metricSearch) {
        this.metricSearch = metricSearch;
    }
}

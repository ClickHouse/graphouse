package ru.yandex.market.graphouse.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Resource;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
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
                ban(req, resp);
                break;
            default:
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp.getOutputStream().println("Usage:");
                resp.getOutputStream().println("/search?query=<searchquery>");
                resp.getOutputStream().println("/ban?name=<metricname>");
                break;
        }
    }

    private void ban(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String metric = req.getParameter("name");
        if (metric == null || metric.isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getOutputStream().println("Usage: /ban?name=<metricname>");
            return;
        }
        metricSearch.ban(metric);
        resp.getOutputStream().println("Baned: " + metric);
    }

    private void search(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String query = req.getParameter("query");
        if (query == null || query.isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getOutputStream().println("Usage: /search?query=<searchquery>");
            return;
        }
        String answer = metricSearch.search(query);
        resp.getOutputStream().println(answer);
    }

    @Resource
    public void setMetricSearch(MetricSearch metricSearch) {
        this.metricSearch = metricSearch;
    }
}

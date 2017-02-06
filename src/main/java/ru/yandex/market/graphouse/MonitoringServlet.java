package ru.yandex.market.graphouse;

import org.springframework.beans.factory.annotation.Required;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.search.MetricSearch;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 27/04/15
 */
public class MonitoringServlet extends HttpServlet {
    private Monitoring monitoring;
    private MetricSearch metricSearch;

    private boolean allowColdRun = false;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        switch (req.getRequestURI()) {
            case "/ping":
                ping(resp);
                break;
            case "/monitoring":
                monitoring(resp);
                break;
            default:
                resp.getWriter().print("Bad request");
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                break;
        }
    }

    private void ping(HttpServletResponse resp) throws IOException {
        if (allowColdRun || metricSearch.isMetricTreeLoaded()) {
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().print("0;OK");
            return;
        }

        resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        resp.getWriter().println("2;Metric tree not loaded ");
    }

    private void monitoring(HttpServletResponse resp) throws IOException {
        Monitoring.Result result = monitoring.getResult();
        switch (result.getStatus()) {
            case OK:
            case WARNING:
                resp.setStatus(HttpServletResponse.SC_OK);
                break;
            case CRITICAL:
                resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                break;
            default:
                throw new IllegalStateException();
        }
        resp.getWriter().print(result.toString());
    }

    @Required
    public void setMonitoring(Monitoring monitoring) {
        this.monitoring = monitoring;
    }

    @Required
    public void setMetricSearch(MetricSearch metricSearch) {
        this.metricSearch = metricSearch;
    }

    public void setAllowColdRun(boolean allowColdRun) {
        this.allowColdRun = allowColdRun;
    }
}

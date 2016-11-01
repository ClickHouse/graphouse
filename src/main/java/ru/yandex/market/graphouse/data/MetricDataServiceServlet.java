package ru.yandex.market.graphouse.data;

import org.apache.commons.lang3.StringUtils;
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
    private MetricDataService metricDataService;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        final String metricsString = req.getParameter("metrics");

        if (StringUtils.isEmpty(metricsString)) {
            log.warn("Metrics list is empty");
            writeBadRequest(resp);
            return;
        }

        final List<String> metrics = Arrays.asList(metricsString.split(","));

        final int startTimeSeconds;
        final int endTimeSeconds;

        try {
            startTimeSeconds = Integer.parseInt(req.getParameter("startSecond"));
            endTimeSeconds = Integer.parseInt(req.getParameter("endSecond"));
        } catch (NumberFormatException e) {
            log.warn("Integer parameters parsing failed", e);
            writeBadRequest(resp);
            return;
        }

        final MetricDataParameters parameters = new MetricDataParameters(metrics, startTimeSeconds, endTimeSeconds);

        parameters.setReqKey(req.getParameter("reqKey"));

        metricDataService.writeData(parameters, resp.getWriter());
        resp.setStatus(HttpServletResponse.SC_OK);
    }

    private void writeBadRequest(HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getOutputStream().println("Usage:");
        resp.getOutputStream().println("/metricData?");
        resp.getOutputStream().println("\t metrics=<metric1,metric2...>");
        resp.getOutputStream().println("\t &startSecond=<startTime>");
        resp.getOutputStream().println("\t &endSecond=<endTime>");
        resp.getOutputStream().println("\t [&reqKey=<reqKey>]");
    }

    public void setMetricDataService(MetricDataService metricDataService) {
        this.metricDataService = metricDataService;
    }
}

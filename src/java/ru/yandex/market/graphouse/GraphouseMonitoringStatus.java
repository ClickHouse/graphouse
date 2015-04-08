package ru.yandex.market.graphouse;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 06/04/15
 */
public class GraphouseMonitoringStatus {


    private volatile boolean metricCacher = true;
    private volatile boolean MetricServer = true;

    public boolean isMetricCacher() {
        return metricCacher;
    }

    public void setMetricCacher(boolean metricCacher) {
        this.metricCacher = metricCacher;
    }

    public boolean isMetricServer() {
        return MetricServer;
    }

    public void setMetricServer(boolean metricServer) {
        MetricServer = metricServer;
    }
}

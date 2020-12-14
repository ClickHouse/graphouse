package ru.yandex.market.graphouse.utils;

import ru.yandex.market.graphouse.search.tree.InMemoryMetricDir;
import ru.yandex.market.graphouse.search.tree.MetricDescription;
import ru.yandex.market.graphouse.search.tree.MetricDir;

import java.io.IOException;

/**
 * @author Mishunin Andrei <a href="mailto:mishunin@yandex-team.ru"></a>
 * @date 14.12.2020
 */
public class TraceAppendableWrapper implements AppendableResult {
    private final Appendable writer;
    private final boolean writeLoadedInfo;

    public TraceAppendableWrapper(Appendable writer, boolean writeLoadedInfo) {
        this.writer = writer;
        this.writeLoadedInfo = writeLoadedInfo;
    }

    @Override
    public void appendMetric(MetricDescription metric) throws IOException {
        writer.append(metric.getName())
            .append(" type:")
            .append(getMetricType(metric))
            .append(", status:")
            .append(metric.getStatus().name());

        if (writeLoadedInfo && metric instanceof MetricDir) {
            appendLoadedInfo((MetricDir) metric);
        }

        writer.append("\n");
    }

    @Override
    public String toString() {
        return writer.toString();
    }

    private String getMetricType(MetricDescription metric) {
        return metric instanceof InMemoryMetricDir ? "IN_MEMORY" : "LOADABLE";
    }

    private void appendLoadedInfo(MetricDir dir) throws IOException {
        writer.append(", loaded dirs: ")
            .append(Integer.toString(dir.loadedDirCount()))
            .append(", loaded metrics: ")
            .append(Integer.toString(dir.loadedMetricCount()));
    }
}

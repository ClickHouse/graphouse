package ru.yandex.market.graphouse.utils;

import ru.yandex.market.graphouse.search.MetricDescription;

import java.io.IOException;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 08.11.16
 */
public class AppendableWrapper implements AppendableResult {
    private final Appendable writer;

    public AppendableWrapper() {
        this.writer = new StringBuilder();
    }

    public AppendableWrapper(Appendable writer) {
        this.writer = writer;
    }

    @Override
    public void appendMetric(MetricDescription metric) throws IOException {
        writer.append(metric.getName()).append("\n");
    }

    @Override
    public String toString() {
        return writer.toString();
    }
}

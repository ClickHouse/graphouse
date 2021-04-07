package ru.yandex.market.graphouse.utils;

import ru.yandex.market.graphouse.save.tree.OnReadDirContent;
import ru.yandex.market.graphouse.save.tree.OnRecordMetricDescription;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.io.IOException;

public class OnReadAppendableResult implements AppendableResult {
    private final Appendable writer;

    public OnReadAppendableResult(Appendable writer) {
        this.writer = writer;
    }

    @Override
    public void appendMetric(MetricDescription metric) throws IOException {
        writer.append(metric.getName())
            .append(" type:")
            .append("ON_RECORD_CACHED")
            .append(", status:")
            .append(metric.getStatus().name());

        writer.append("\n");
    }

    public void appendMetric(OnRecordMetricDescription metric, OnReadDirContent parentContent) throws IOException {
        if (metric == null) {
            writer.append("not_in_cached_tree");
        } else {
            writer.append(metric.getName())
                .append(" type:")
                .append("ON_RECORD_CACHED")
                .append(", status:")
                .append(metric.getStatus().name())
                .append(", maybe new: ")
                .append(Boolean.toString(metric.isMaybeNewMetrics()))
                .append(", content loaded: ")
                .append(Boolean.toString(metric.isContentLoaded()));
        }

        writer.append(", parent content loaded: ")
            .append(Boolean.toString(parentContent.isLoaded()))
            .append("\n");
    }

    public void appendNotSimpleStatus(String name, MetricStatus status) throws IOException {
        writer.append(name)
            .append(" type:")
            .append("BAN_CACHE")
            .append(", status:")
            .append(status.name());

        writer.append("\n");
    }
}

package ru.yandex.market.graphouse.cacher;

import ru.yandex.market.graphouse.Metric;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 14/02/15
 */
public class MetricInputStream extends InputStream {

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US);
    private List<Metric> metrics;

    private StringBuilder row = new StringBuilder(100);
    private int rowNum = 0;
    private int rowPosition = 0;

    public MetricInputStream(List<Metric> metrics) {
        this.metrics = metrics;
    }

    @Override
    public int read() throws IOException {
        if (rowPosition >= row.length()) {
            readRow();
        }
        if (rowPosition >= row.length()) {
            return -1;
        }
        return row.codePointAt(rowPosition++);
    }

    private void readRow() {
        if (rowNum >= metrics.size()) {
            return;
        }

        rowPosition = 0;
        row.setLength(0);

        Metric metric = metrics.get(rowNum);

        row.append(metric.getName()).append('\t');
        row.append(metric.getValue()).append('\t');
        row.append(metric.getTime()).append('\t');
        row.append(dateFormat.format(metric.getUpdated())).append('\t');
        row.append(getTimestampSeconds(metric.getUpdated())).append('\n');
        rowNum++;
    }

    private static int getTimestampSeconds(Date date) {
        return (int) (date.getTime() / 1000);
    }
}

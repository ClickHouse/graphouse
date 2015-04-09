package ru.yandex.market.graphouse.cacher;

import org.apache.http.util.ByteArrayBuffer;
import ru.yandex.market.graphouse.Metric;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
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

    ByteBuffer buffer = ByteBuffer.allocate(2000);
    private int rowNum = 0;

    public MetricInputStream(List<Metric> metrics) {
        this.metrics = metrics;
    }

    @Override
    public int read() throws IOException {
        if (!fillBuffer()) {
            return -1;
        }
        return buffer.get();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (!fillBuffer()) {
            return -1;
        }
        int remaining = buffer.remaining();
        buffer.put(b, off, len);
        return remaining - buffer.remaining();
    }

    /**
     * @return false если данные закончились
     */
    private boolean fillBuffer() {
        if (buffer.remaining() == 0) {
            readRow();
        }
        return buffer.remaining() > 0;
    }

    private void readRow() {
        if (rowNum >= metrics.size()) {
            return;
        }

        Metric metric = metrics.get(rowNum);

        appendBytes(metric.getName().getBytes());
        buffer.putChar('\t');
        appendBytes(Double.toString(metric.getValue()).getBytes());
        buffer.putChar('\t');
        append(metric.getTime());
        buffer.putChar('\t');
        appendBytes(dateFormat.format(metric.getUpdated()).getBytes());
        buffer.putChar('\t');
        append(getTimestampSeconds(metric.getUpdated()));
        buffer.putChar('\n');
        rowNum++;
    }

    private void append(int data) {
        appendBytes(Integer.toString(data).getBytes());
    }

    private void appendBytes(byte[] bytes) {
        buffer.put(bytes, 0, bytes.length);
    }

    private static int getTimestampSeconds(Date date) {
        return (int) (date.getTime() / 1000);
    }
}

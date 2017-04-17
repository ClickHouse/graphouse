package ru.yandex.market.graphouse.cacher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.LittleEndianDataOutputStream;
import org.apache.http.entity.AbstractHttpEntity;
import ru.yandex.market.graphouse.Metric;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Write in ClickHouse in a fast RowBinary format
 * See https://clickhouse.yandex/reference_en.html#RowBinary for details
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 16/04/2017
 */
public class MetricRowBinaryHttpEntity extends AbstractHttpEntity {

    private static final LocalDate EPOCH = LocalDate.ofEpochDay(0);

    private final int todayStartSeconds;
    private final int todayEndSeconds;
    private final short currentDay;

    private final List<Metric> metrics;

    @VisibleForTesting
    protected MetricRowBinaryHttpEntity(List<Metric> metrics, LocalDate localDate) {
        this.metrics = metrics;
        //Optimization. Assume that all metrics are today and precalc day number.
        currentDay = (short) Short.toUnsignedInt((short) localDate.toEpochDay());
        todayStartSeconds = (int) localDate.atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        todayEndSeconds = (int) localDate.plusDays(1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
    }

    public MetricRowBinaryHttpEntity(List<Metric> metrics) {
        this(metrics, LocalDate.now());
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
        LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(outputStream);
        for (Metric metric : metrics) {
            writeMetric(metric, out);
        }
    }

    /**
     * (metric, value, timestamp, date, updated)
     *
     * @param metric
     * @param out
     * @throws IOException
     */
    private void writeMetric(Metric metric, LittleEndianDataOutputStream out) throws IOException {
        writeUnsignedLeb128(metric.getMetricDescription().getNameLength(), out);
        metric.getMetricDescription().writeName(out);
        out.writeDouble(metric.getValue());
        out.writeInt((int) Integer.toUnsignedLong(metric.getTimestampSeconds()));
        out.writeShort(getUnsignedDaysSinceEpoch(metric.getTimestampSeconds()));
        out.writeInt((int) Integer.toUnsignedLong(metric.getUpdatedSeconds()));
    }

    @VisibleForTesting
    protected short getUnsignedDaysSinceEpoch(int timestampSeconds) {
        if (timestampSeconds >= todayStartSeconds && timestampSeconds < todayEndSeconds) {
            return currentDay;
        }
        LocalDate localDate = Instant.ofEpochSecond(timestampSeconds).atZone(ZoneId.systemDefault()).toLocalDate();
        short days = (short) ChronoUnit.DAYS.between(EPOCH, localDate);
        return (short) Short.toUnsignedInt(days);
    }

    private static void writeUnsignedLeb128(int value, LittleEndianDataOutputStream out) throws IOException {
        int remaining = value >>> 7;
        while (remaining != 0) {
            out.write((byte) ((value & 0x7f) | 0x80));
            value = remaining;
            remaining >>>= 7;
        }
        out.write((byte) (value & 0x7f));
    }

    @Override
    public boolean isStreaming() {
        return false;
    }

    @VisibleForTesting
    protected int getTodayStartSeconds() {
        return todayStartSeconds;
    }

    @VisibleForTesting
    protected int getTodayEndSeconds() {
        return todayEndSeconds;
    }

    @VisibleForTesting
    protected short getCurrentDay() {
        return currentDay;
    }
}

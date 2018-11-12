package ru.yandex.market.graphouse.cacher;

import com.google.common.annotations.VisibleForTesting;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;
import ru.yandex.market.graphouse.Metric;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.TimeZone;

/**
 * Write in ClickHouse in a fast RowBinary format.
 * See https://clickhouse.yandex/reference_en.html#RowBinary for details
 *
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 16/04/2017
 */
public class MetricsStreamCallback implements ClickHouseStreamCallback {

    private static final LocalDate EPOCH = LocalDate.ofEpochDay(0);


    private final int todayStartSeconds;
    private final int todayEndSeconds;
    private final short currentDay;

    private final List<Metric> metrics;
    private final ZoneId clickHouseZoneId;

    @VisibleForTesting
    protected MetricsStreamCallback(List<Metric> metrics, ZoneId clickHouseZoneId, LocalDate localDate) {
        this.metrics = metrics;
        this.clickHouseZoneId = clickHouseZoneId;
        //Optimization. Assume that all metrics are today and precalc day number.
        currentDay = (short) Short.toUnsignedInt((short) localDate.toEpochDay());
        todayStartSeconds = (int) localDate.atStartOfDay(clickHouseZoneId).toEpochSecond();
        todayEndSeconds = (int) localDate.plusDays(1).atStartOfDay(clickHouseZoneId).toEpochSecond();
    }

    public MetricsStreamCallback(List<Metric> metrics, TimeZone clickHouseTimeZone) {
        this(metrics, clickHouseTimeZone.toZoneId(), LocalDate.now());
    }

    public MetricsStreamCallback(List<Metric> metrics, ZoneId clickHouseZoneId) {
        this(metrics, clickHouseZoneId, LocalDate.now());
    }

    @Override
    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
        for (Metric metric : metrics) {
            writeMetric(metric, stream);
        }
    }

    /**
     * (metric, value, timestamp, date, updated).
     *
     * @param metric
     * @param stream
     * @throws IOException
     */
    private void writeMetric(Metric metric, ClickHouseRowBinaryStream stream) throws IOException {
        stream.writeUnsignedLeb128(metric.getMetricDescription().getNameLengthInBytes());
        metric.getMetricDescription().writeName(stream);
        stream.writeFloat64(metric.getValue());
        stream.writeUInt32(metric.getTimestampSeconds());
        stream.writeUInt16(getUnsignedDaysSinceEpoch(metric.getTimestampSeconds()));
        stream.writeUInt32((int) Integer.toUnsignedLong(metric.getUpdatedSeconds()));
    }

    @VisibleForTesting
    protected short getUnsignedDaysSinceEpoch(int timestampSeconds) {
        if (timestampSeconds >= todayStartSeconds && timestampSeconds < todayEndSeconds) {
            return currentDay;
        }
        LocalDate localDate = Instant.ofEpochSecond(timestampSeconds).atZone(clickHouseZoneId).toLocalDate();
        short days = (short) ChronoUnit.DAYS.between(EPOCH, localDate);
        return (short) Short.toUnsignedInt(days);
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

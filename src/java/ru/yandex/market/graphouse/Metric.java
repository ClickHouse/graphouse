package ru.yandex.market.graphouse;

import ru.yandex.market.graphouse.search.MetricDescription;

import java.util.Date;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 06/04/15
 */
public class Metric {
    private final MetricDescription name;
    private final Date time;
    private final double value;
    private final int updated;

    public Metric(MetricDescription name, Date time, double value, int updated) {
        this.name = name;
        this.time = time;
        this.value = value;
        this.updated = updated;
    }

    public MetricDescription getName() {
        return name;
    }

    public Date getTime() {
        return time;
    }

    public double getValue() {
        return value;
    }

    public int getUpdated() {
        return updated;
    }
}

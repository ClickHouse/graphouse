package ru.yandex.market.graphouse;

import java.util.Date;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 06/04/15
 */
public class Metric {
    private final String name;
    private final int time;
    private final double value;
    private final Date updated;

    public Metric(String name, int time, double value, Date updated) {
        this.name = name;
        this.time = time;
        this.value = value;
        this.updated = updated;
    }

    public String getName() {
        return name;
    }

    public int getTime() {
        return time;
    }

    public double getValue() {
        return value;
    }

    public Date getUpdated() {
        return updated;
    }
}

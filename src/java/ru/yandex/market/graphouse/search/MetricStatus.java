package ru.yandex.market.graphouse.search;

import java.util.NoSuchElementException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 08/06/15
 */
public enum MetricStatus {

    SIMPLE(0),
    BAN(1),
    APPROVED(2),
    HIDDEN(3),
    AUTO_HIDDEN(4);

    private final int id;

    MetricStatus(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public boolean visible() {
        switch (this) {
            case SIMPLE:
            case APPROVED:
                return true;
            case BAN:
            case HIDDEN:
            case AUTO_HIDDEN:
                return false;
            default:
                throw new IllegalStateException();
        }
    }

    public boolean handmade() {
        switch (this) {
            case APPROVED:
            case BAN:
            case HIDDEN:
                return true;
            case SIMPLE:
            case AUTO_HIDDEN:
                return false;
            default:
                throw new IllegalStateException();
        }
    }


    public static MetricStatus forId(int id) {
        for (MetricStatus status : MetricStatus.values()) {
            if (status.getId() == id) {
                return status;
            }
        }
        throw new NoSuchElementException("No MetricStatus for id " + id);
    }
}

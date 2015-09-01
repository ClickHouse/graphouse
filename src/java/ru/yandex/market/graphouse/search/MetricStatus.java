package ru.yandex.market.graphouse.search;

import java.util.NoSuchElementException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 08/06/15
 */
public enum MetricStatus {
    /**
     * Статус по умолчанию при создании директории/метрики.
     */
    SIMPLE(0),
    /**
     * Если метрика забанена, то
     * - метрика перестаёт находиться в поиске (а следовательно и в графите)
     * - значения метрики перестают приниматься и писаться в графит
     * Если забанена директория, то в неё нельзя сохранить метрики.
     */
    BAN(1),
    APPROVED(2),
    HIDDEN(3),
    /**
     * Метрика автоматически скрыта в {@link ru.yandex.market.graphouse.AutoHideService}
     */
    AUTO_HIDDEN(4);

    private final int id;

    MetricStatus(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    /**
     * Если <code>false</code>, то в поиске не будет отдаваться данная метрика (ни одна метрика из данной директории)
     * @return
     */
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

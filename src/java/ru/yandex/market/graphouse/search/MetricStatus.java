package ru.yandex.market.graphouse.search;

import java.util.*;

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
     * Если директория (метрика) забанена, то
     * - директория и все метрики в ней (метрика) перестаёт находиться в поиске (а следовательно и в графите)
     * - значения метрик в директории (метрики) перестают приниматься и писаться в графит
     */
    BAN(1),
    APPROVED(2),
    /**
     * Если директория(метрика) скрыта, то
     * - директория и все метрики в ней (метрика) перестаёт находиться в поиске (а следовательно и в графите)
     * - значения метрик продолжают приниматься
     *
     * Чтобы открыть деректорию(метрику), необходимо явно перевести в {@link #APPROVED}
     */
    HIDDEN(3),
    /**
     * Директория автоматически скрывается, если все её дочерние директории и метрики не видимы {@link #visible}
     * Как только появится новое значение для дочерней метрики, директория будет открыта {@link #SIMPLE}
     *
     * Метрика может быть автоматически скрыта в {@link ru.yandex.market.graphouse.AutoHideService}
     * Аналогично, при появлении новых значений будет открыта {@link #SIMPLE}
     */
    AUTO_HIDDEN(4);

    private final int id;

    MetricStatus(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static final Map<MetricStatus, List<MetricStatus>> RESTRICTED_GRAPH_EDGES = new HashMap<>();
    static {
        RESTRICTED_GRAPH_EDGES.put(MetricStatus.BAN, Arrays.asList(MetricStatus.HIDDEN, MetricStatus.AUTO_HIDDEN));
        RESTRICTED_GRAPH_EDGES.put(MetricStatus.APPROVED, Arrays.asList(MetricStatus.SIMPLE));
        RESTRICTED_GRAPH_EDGES.put(MetricStatus.HIDDEN, Arrays.asList(MetricStatus.SIMPLE, MetricStatus.AUTO_HIDDEN));
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

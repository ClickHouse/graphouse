package ru.yandex.market.graphouse.search;

/**
 * Статус, показывающий удалось ли создать / изменить метрику/директорию.
 *
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 20/04/15
 */
public enum QueryStatus {
    /**
     * создана новая метрика (для директорий всегда UPDATED или UNMODIFIED)
     */
    NEW,
    UPDATED,
    UNMODIFIED,
    /**
     * изменение не допустимо
     */
    WRONG,
    /**
     * директория данной метрики забанена, поэтому операция не выполняется
     */
    BAN
}
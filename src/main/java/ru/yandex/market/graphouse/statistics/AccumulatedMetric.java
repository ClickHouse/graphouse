package ru.yandex.market.graphouse.statistics;

/**
 * @author Nikolay Firov <a href="mailto:firov@yandex-team.ru"></a>
 * @date 22.12.17
 */
public enum AccumulatedMetric {
    NUMBER_OF_RECEIVED_METRICS,
    NUMBER_OF_INVALID_METRICS, // todo: not implemented yet. Possibly we need instant metric for this
    NUMBER_OF_WRITTEN_METRICS,
    NUMBER_OF_WRITE_ERRORS,
    NUMBER_OF_WEB_REQUESTS,
    NUMBER_OF_LOADED_DIRS,
    NUMBER_OF_LOADED_METRICS,
    NUMBER_OF_UPDATED_METRIC_TREE_NODES
}

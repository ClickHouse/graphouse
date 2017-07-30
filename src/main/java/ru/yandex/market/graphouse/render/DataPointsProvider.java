package ru.yandex.market.graphouse.render;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 16/07/2017
 */
public interface DataPointsProvider {

    DataPoints getDataPoints(DataPointsParams params);

    default DataPoints getDataPoints() {
        return getDataPoints(getDataPointsParams());
    }

    DataPointsParams getDataPointsParams();
}

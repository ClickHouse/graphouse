package ru.yandex.market.graphouse.render;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 15/05/2017
 */
public interface FunctionContext {

    default DataPoints getDataPoints(int index) {
        return getDataPoints(index, getDataPointsParams());
    }

    DataPoints getDataPoints(int index, DataPointsParams params);

    DataPointsParams getDataPointsParams();

    int getIntParam(int index);

    String getStringParam(int index);
}

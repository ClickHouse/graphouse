package ru.yandex.market.graphouse.render;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 13/02/2017
 */
public interface Function {

    default DataPointsParams getDataPointParams(FunctionContext context) {
        return context.getDataPointsParams();
    }

    DataPoints apply(FunctionContext context);

    Type getType();

    enum Type {
        DATA,
        DATAPOINTS_WITH_PARAMS,
        DATAPOINTS_LIST;
    }
}

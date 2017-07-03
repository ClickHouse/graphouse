package ru.yandex.market.graphouse.render;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 13/02/2017
 */
public interface Function {

    DataPoints apply(FunctionContext functionContext);

    Type getType();

    enum Type{
        DATAPOINTS_WITH_PARAMS;
    }
}

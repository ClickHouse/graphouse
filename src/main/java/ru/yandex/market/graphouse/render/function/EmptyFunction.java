package ru.yandex.market.graphouse.render.function;

import ru.yandex.market.graphouse.render.DataPoints;
import ru.yandex.market.graphouse.render.Function;
import ru.yandex.market.graphouse.render.FunctionContext;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 15/02/2017
 */
public class EmptyFunction implements Function {

    @Override
    public DataPoints apply(FunctionContext context) {
        return context.getDataPoints(0);
    }

    @Override
    public Type getType() {
        return Type.DATAPOINTS_WITH_PARAMS;
    }
}

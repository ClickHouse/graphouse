package ru.yandex.market.graphouse.render.function;

import ru.yandex.market.graphouse.render.DataPoints;
import ru.yandex.market.graphouse.render.Function;
import ru.yandex.market.graphouse.render.FunctionContext;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 15/02/2017
 */
public class AggregationFunction implements Function {

    private final String graphiteName;
    private final String clickHouseFunction;

    public AggregationFunction(String graphiteName, String clickHouseFunction) {
        this.graphiteName = graphiteName;
        this.clickHouseFunction = clickHouseFunction;
    }

    @Override
    public DataPoints apply(FunctionContext context) {
        DataPoints dataPoints = context.getDataPoints(0);
        String query = String.format(
            "SELECT ('%s(' || '%s' || ')') AS name, %s(values) FROM (%s)",
            graphiteName, dataPoints.getName(), clickHouseFunction, dataPoints.getQuery()
        );
        return new DataPoints("name", query, dataPoints.getParams());
    }

    @Override
    public Type getType() {
        return Type.DATAPOINTS_WITH_PARAMS;
    }
}

package ru.yandex.market.graphouse.render.function;

import ru.yandex.market.graphouse.render.DataPoints;
import ru.yandex.market.graphouse.render.Function;
import ru.yandex.market.graphouse.render.FunctionContext;

import java.util.stream.Collectors;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 19/02/2017
 */
public class AliasByNodeFunction implements Function {

    @Override
    public Type getType() {
        return Type.DATAPOINTS_WITH_PARAMS;
    }

    @Override
    public DataPoints apply(FunctionContext context) {
        DataPoints dataPoints = context.getDataPoints(0);
        String query = String.format(
            "SELECT arrayStringConcat(array(%s)), '.') as name, values FROM SELECT * FROM (%s)",
            context.getParams().stream().map(p -> extractNode(p, dataPoints.getName())).collect(Collectors.joining(",")),
            dataPoints.getQuery()
        );
        return new DataPoints("name", query, dataPoints.getParams());
    }

    private static String extractNode(String index, String name) {
        return "splitByChar('.', " + name + ")[" + index + " + 1]";
    }
}

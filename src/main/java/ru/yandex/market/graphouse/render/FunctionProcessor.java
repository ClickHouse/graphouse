package ru.yandex.market.graphouse.render;

import com.google.common.base.Splitter;
import ru.yandex.market.graphouse.render.function.AggregationFunction;
import ru.yandex.market.graphouse.render.function.EmptyFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 13/02/2017
 */
public class FunctionProcessor {

    private static final Splitter ARGS_SPLITTER = Splitter.on(',').trimResults();

    private final Map<String, Function> functions = new HashMap<>();

    public FunctionProcessor() {

        addFunction(new AggregationFunction("sumSeries", "sum"), "sumSeries", "sum");
        addFunction(new AggregationFunction("averageSeries", "avg"), "averageSeries", "average", "avgSeries", "avg");
        addFunction(new AggregationFunction("minSeries", "min"), "minSeries", "min");
        addFunction(new AggregationFunction("maxSeries", "max"), "maxSeries", "max");
        addFunction(new EmptyFunction(), "color");

    }

    private void addFunction(Function function, String... names) {
        for (String name : names) {
            functions.put(name.toLowerCase(), function);
        }
    }

    //aliasByNode(averageAbove(one_min.market-front.errors-dynamic.5xx-percent.*.*, 0.1), 3, 5)
    public DataPointsProvider parse(String request, RenderContext context) throws RequestParseException {
        int startBraceIndex = request.indexOf('(');
        int endBraceIndex = request.lastIndexOf(')');
        if (startBraceIndex > 0 ^ endBraceIndex > 0) {
            throw new RequestParseException("Wrong braces", request);
        }
        boolean hasSubFunction = startBraceIndex > 0 && endBraceIndex > 0;
        if (!hasSubFunction) {
            return new QueryDataPointsProvider(request, context);
        }
        String functionName = request.substring(0, startBraceIndex);
        Function function = functions.get(functionName.toLowerCase());
        if (function == null) {
            throw new RequestParseException("Unknown function: " + functionName, request);
        }

        String argsString = request.substring(startBraceIndex + 1, endBraceIndex).trim();
        List<String> args = splitArgs(argsString);
        switch (function.getType()) {
            case DATA:
                return new FunctionDataPointsProvider(function, context, Collections.emptyList(), args);
            case DATAPOINTS_WITH_PARAMS:
                DataPointsProvider subProvider = parse(args.get(0), context);
                return new FunctionDataPointsProvider(
                    function, context, Collections.singletonList(subProvider), args.subList(1, args.size())
                );
            case DATAPOINTS_LIST:
                List<DataPointsProvider> subProviders = new ArrayList<>(args.size());
                for (String arg : args) {
                    subProviders.add(parse(arg, context));
                }
                return new FunctionDataPointsProvider(function, context, subProviders, Collections.emptyList());
            default:
                throw new IllegalStateException();

        }

    }

    private List<String> splitArgs(String string) {
        return ARGS_SPLITTER.splitToList(string);
    }

}

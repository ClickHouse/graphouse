package ru.yandex.market.graphouse.render;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import ru.yandex.market.graphouse.render.function.AggregationFunction;
import ru.yandex.market.graphouse.render.function.EmptyFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 13/02/2017
 */
public class FunctionProcessor {

    private static final Splitter ARGS_SPLITTER = Splitter.on(',').trimResults();

    private final Map<String, Function> functions = new HashMap<>();

    public FunctionProcessor() {
//        addFunction(
//            new Function() {
//                @Override
//                public void apply(QueryBuilder queryBuilder, String... args) {
//                    queryBuilder.groupToArrays("avgIf(value, isFinite(value)) > " + args[0]);
//                }
//            },
//            "averageAbove"
//        );

        addFunction(new AggregationFunction("sumSeries", "sum"), "sumSeries", "sum");
        addFunction(new AggregationFunction("avgSeries", "avg"), "avgSeries", "avg");
//        addFunction(new AliasByNodeFunction(), "aliasByNode");

//        addFunction(
//            new Function() {
//                @Override
//                public void apply(QueryBuilder queryBuilder, String... args) {
//                    String start = args[1];
//                    String end = args[2];
//                    queryBuilder.addMutation(
//                        "arrayStringConcat(" +
//                            "   arrayFilter(" +
//                            "       (level, num) -> (num  BETWEEN " + start + " and " + end + "), " +
//                            "       splitByChar('.', 'a.b.c.d.e') as tmp1, arrayEnumerate(tmp1)" +
//                            "   )," +
//                            " '.'" +
//                            ") as metric"
//                    );
//                }
//            },
//            "aliasByNode"
//        );

        addFunction(new EmptyFunction(), "color");
    }

    private void addFunction(Function function, String... names) {
        for (String name : names) {
            functions.put(name.toLowerCase(), function);
        }
    }

    //aliasByNode(averageAbove(one_min.market-front.errors-dynamic.5xx-percent.*.*, 0.1), 3, 5)
    public FunctionWrapper parse(String request) throws RequestParseException {
        int startBraceIndex = request.indexOf('(');
        int endBraceIndex = request.lastIndexOf(')');
        if (startBraceIndex > 0 ^ endBraceIndex > 0) {
            throw new RequestParseException("Wrong braces", request);
        }
        boolean hasSubFunction = startBraceIndex > 0 && endBraceIndex > 0;
        if (!hasSubFunction) {
            return new FunctionWrapper(DataFunction.INSTANCE, new String[]{request});
        }
        String functionName = request.substring(0, startBraceIndex);
        Function function = functions.get(functionName.toLowerCase());
        if (function == null) {
            throw new RequestParseException("Unknown function: " + functionName, request);
        }

        String argsString = request.substring(startBraceIndex + 1, endBraceIndex).trim();
        if (!function.hasSeries()) {
            return new FunctionWrapper(function, splitArgs(argsString));
        }
        if (argsString.isEmpty()) {
            throw new RequestParseException("seriesList not provided for function: " + functionName, request);
        }

        boolean brace = true;
        int splitIndex = argsString.lastIndexOf(')');
        if (splitIndex == -1) {
            brace = false;
            splitIndex = argsString.indexOf(',');
        }
        if (splitIndex == -1) {
            splitIndex = 0;
        }

        String subFunctionString = argsString.substring(0, brace ? splitIndex + 1 : splitIndex);
        FunctionWrapper subFunction = parse(subFunctionString);
        String[] args = splitArgs(argsString.substring(splitIndex + 2));
        return new FunctionWrapper(function, args, subFunction);

    }

    private String[] splitArgs(String string) {
        return Iterables.toArray(ARGS_SPLITTER.split(string), String.class);
    }

    @VisibleForTesting
    protected static class FunctionWrapper {
        private final Function function;
        private final String[] args;
        private final FunctionWrapper subFunction;

        public FunctionWrapper(Function function, String[] args, FunctionWrapper subFunction) {
            this.function = function;
            this.args = args;
            this.subFunction = subFunction;
        }

        public FunctionWrapper(Function function, String[] args) {
            this(function, args, null);
        }

        public Function getFunction() {
            return function;
        }

        public String[] getArgs() {
            return args;
        }

        public FunctionWrapper getSubFunction() {
            return subFunction;
        }
    }

}

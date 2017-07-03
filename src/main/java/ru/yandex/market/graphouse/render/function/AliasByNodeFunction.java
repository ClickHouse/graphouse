//package ru.yandex.market.graphouse.render.function;
//
//import ru.yandex.market.graphouse.render.DataPoints;
//import ru.yandex.market.graphouse.render.Function;
//import ru.yandex.market.graphouse.render.FunctionContext;
//import ru.yandex.market.graphouse.render.QueryBuilder;
//
//import java.util.Arrays;
//import java.util.stream.Collectors;
//
///**
// * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
// * @date 19/02/2017
// */
//public class AliasByNodeFunction implements Function {
//
//    @Override
//    public DataPoints apply(FunctionContext functionContext) {
//        return null;
//    }
//
//    @Override
//    public void apply(QueryBuilder queryBuilder, String... args) {
//        queryBuilder.addMetricAlias(
//            "arrayStringConcat(array(" +
//                Arrays.stream(args).map(AliasByNodeFunction::extractNode).collect(Collectors.joining(",")) +
//                "), '.')"
//        );
//    }
//
//    private static String extractNode(String index) {
//        return "splitByChar('.', metric)[" + index + " + 1]";
//    }
//
////    @Override
////    public void apply(QueryBuilder queryBuilder, String... args) {
////
////        String nodes = Arrays.stream(args).collect(Collectors.joining(","));
////        queryBuilder.addMetricAlias(
////            "arrayStringConcat(" +
////                "   arrayFilter(" +
////                "       (level, i) -> (i - 1 IN (" + nodes + ")), splitByChar('.', metric), " +
////                "       arrayEnumerate(splitByChar('.', metric))" +
////                "   )," +
////                "   '.'" +
////                ")"
////        );
////    }
//}

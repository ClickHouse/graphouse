//package ru.yandex.market.graphouse.render.function;//package ru.yandex.market.graphouse.render.function;
//
//import ru.yandex.market.graphouse.render.Function;
//import ru.yandex.market.graphouse.render.QueryBuilder;
//
///**
// * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
// * @date 13/02/2017
// */
//public class HavingFunction extends Function {
//
//    private final String havingQuery;
//
//
//
//    public HavingFunction(String havingQuery) {
//        super(true);
//        this.havingQuery = havingQuery;
//    }
//
//    @Override
//    public void apply(QueryBuilder queryBuilder, String... args) {
//        queryBuilder.groupToArrays(havingQuery);
//    }
//}

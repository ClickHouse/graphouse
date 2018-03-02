package ru.yandex.market.graphouse.render;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 15/05/2017
 */
public class DataPoints {

    private final String name;

    private final String query;

    private final DataPointsParams params;

    public DataPoints(String name, String query, DataPointsParams params) {
        this.name = name;
        this.query = query;
        this.params = params;
    }

    public String getName() {
        return name;
    }

    public String getQuery() {
        return query;
    }

    public DataPointsParams getParams() {
        return params;
    }
}

package ru.yandex.market.graphouse.render;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 13/02/2017
 */
public class QueryBuilder {

    public static final String STEP = ":STEP";
    public static final String START = ":START";
    public static final String END = ":END";
    public static final String POINTS = ":POINTS";

    private StringBuilder query = new StringBuilder();
    private String origin;
    private int startTimeSeconds;
    private int endTimeSeconds;
    private int stepSeconds;
    private State state;
    private boolean sortedArray;
    private List<String> metrics;

    private QueryBuilder(StringBuilder query, String origin, List<String> metrics,
                         int startTimeSeconds, int endTimeSeconds, int stepSeconds,
                         State state, boolean sortedArray) {
        this.origin = origin;
        this.metrics = metrics;
        this.startTimeSeconds = startTimeSeconds;
        this.endTimeSeconds = endTimeSeconds;
        this.stepSeconds = stepSeconds;
        this.query = query;
        this.state = state;
        this.sortedArray = sortedArray;
    }

    public static QueryBuilder create(String table, String origin, List<String> metrics, String aggregationFunction,
                                      int startTimeSeconds, int endTimeSeconds, int stepSeconds) {

        StringBuilder builder = new StringBuilder();
        builder.append("SELECT metric, ts, ").append(aggregationFunction).append("(value) as value FROM (\n");

        builder.append("    SELECT metric, ts, argMax(value, updated) as value ");
        builder.append("FROM ").append(table).append(" ");
        builder.append("WHERE metric IN (").append(toMetricString(metrics)).append(") ");
        builder.append("AND ts >= ").append(START).append(" ");
        builder.append("AND ts < ").append(END).append(" ");
        builder.append("AND date >= toDate(").append(START).append(") ");
        builder.append("AND date <= toDate(").append(END).append(") ");
        builder.append("GROUP BY metric, timestamp as ts\n");

        builder.append(") GROUP BY metric, ");
        builder.append("intDiv(toUInt32(ts), ").append(STEP).append(") * ").append(STEP).append(" as ts");


        return new QueryBuilder(
            builder, origin, metrics, startTimeSeconds, endTimeSeconds, stepSeconds, State.VALUES, false
        );

    }

    private static String toMetricString(List<String> metrics) {
        return metrics.stream().collect(Collectors.joining("','", "'", "'"));
    }

    public void requireNanValues() {
        StringBuilder newQuery = new StringBuilder();
        String tss = "arrayJoin(arrayMap(t -> (t * " + STEP + " + " + START + "), range(" + POINTS + ")))";
        String metrics = "arrayJoin(array(" + toMetricString(this.metrics) + "))";

        newQuery.append("SELECT metric, ts, if (has_value, value, nan) FROM \n");
        newQuery.append("\t(SELECT ").append(tss).append(" as ts, ").append(metrics).append(" as metric)\n");
        newQuery.append("ALL LEFT JOIN\n");
        newQuery.append("\t(").append(query).append(")\n");
        newQuery.append("USING (ts, metric)\n");
        query = newQuery;
    }

    public String getOrigin() {
        return origin;
    }

    public void switchToValuesQuery() {
        if (state == State.VALUES) {
            return;
        }
        throw new UnsupportedOperationException();
    }


    public void addAggregation(String valuesMutation, String groupBy) {
        switchToValuesQuery();
        query.insert(0, "SELECT " + valuesMutation + " FROM ( \n");
        query.append("\n) GROUP BY ").append(groupBy);
    }

    public void groupToArrays(String having) {
        switchToValuesQuery();
        query.insert(0, "SELECT metric, groupArray(value) as values, groupArray(ts) as tss FROM ( \n");
        query.append("\n) GROUP BY metric ");
        if (having != null) {
            query.append("HAVING ").append(having).append(" ");
        }

        state = State.ARRAYS;
    }

    public void switchToArraysQuery() {
        if (state == State.ARRAYS) {
            return;
        }
        groupToArrays(null);
    }

    public void addMetricAlias(String alias) {
        StringBuilder newQuery = new StringBuilder();
        newQuery.append("SELECT ").append(alias).append(" AS metric2, ");
        if (state == State.VALUES) {
            newQuery.append("ts, value");
        } else {
            newQuery.append("tss, values");
        }
        newQuery.append(" FROM (\n").append(query).append("\n)");
        query = newQuery;
    }

    public State getState() {
        return state;
    }

    private enum State {
        VALUES,
        ARRAYS
    }

    public String getQuery() {
        switchToArraysQuery();
        return query.toString()
            .replace(START, Integer.toString(startTimeSeconds))
            .replace(END, Integer.toString(endTimeSeconds))
            .replace(STEP, Integer.toString(stepSeconds))
            .replace(POINTS, Integer.toString((endTimeSeconds - startTimeSeconds) / stepSeconds))
            ;
    }
}

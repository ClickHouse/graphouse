package ru.yandex.market.graphouse.perf;

import ru.yandex.market.graphouse.render.DataPoints;
import ru.yandex.market.graphouse.render.DataPointsParams;
import ru.yandex.market.graphouse.render.Function;
import ru.yandex.market.graphouse.render.FunctionContext;

import java.util.Collections;
import java.util.List;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 11/06/2017
 */
@Deprecated
public class FunctionContextImpl implements FunctionContext {

    private DataPointsParams dataPointsParams;
    private List<Function> subFunctions;
    private List<String> args;

    public FunctionContextImpl(DataPointsParams dataPointsParams, Function subFunction) {
        this.dataPointsParams = dataPointsParams;
        this.subFunctions = Collections.singletonList(subFunction);
    }


    @Override
    public DataPoints getDataPoints(int index, DataPointsParams params) {
        return null;
        //        return subFunctions.get(index).;
    }

    @Override
    public DataPointsParams getDataPointsParams() {
        return dataPointsParams;
    }

    @Override
    public int getIntParam(int index) {
        return 0;
    }

    @Override
    public String getStringParam(int index) {
        return null;
    }

    @Override
    public List<String> getParams() {
        return args;
    }
}

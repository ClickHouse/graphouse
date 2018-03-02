package ru.yandex.market.graphouse.render;

import java.util.List;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 17/07/2017
 */
public class FunctionDataPointsProvider implements DataPointsProvider {
    private final Function function;
    private final RenderContext renderContext;
    private final List<DataPointsProvider> dataPointsProviders;
    private final List<String> params;
    private final Context context = new Context();

    public FunctionDataPointsProvider(Function function, RenderContext renderContext,
                                      List<DataPointsProvider> dataPointsProviders, List<String> params) {
        this.function = function;
        this.renderContext = renderContext;
        this.dataPointsProviders = dataPointsProviders;
        this.params = params;
    }

    @Override
    public DataPoints getDataPoints(DataPointsParams params) {
        return function.apply(context);
    }

    @Override
    public DataPointsParams getDataPointsParams() {
        return function.getDataPointParams(context);
    }

    private class Context implements FunctionContext {

        @Override
        public DataPointsParams getDataPointsParams() {
            return dataPointsProviders.get(0).getDataPointsParams();
        }

        @Override
        public DataPoints getDataPoints(int index, DataPointsParams params) {
            return dataPointsProviders.get(index).getDataPoints(params);
        }

        @Override
        public int getIntParam(int index) {
            return Integer.valueOf(params.get(index));
        }

        @Override
        public String getStringParam(int index) {
            return params.get(index);
        }

        @Override
        public List<String> getParams() {
            return params;
        }
    }


}

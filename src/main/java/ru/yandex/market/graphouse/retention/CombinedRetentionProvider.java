package ru.yandex.market.graphouse.retention;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Mikhail f. Shiryaev <a href="mailto:mr.felixoid@gmail.com"></a>
 * @date 07/03/2019
 */
public class CombinedRetentionProvider implements RetentionProvider {
    private final List<MetricRetention> configRetentions;
    private final List<MetricRetention> combinedRetentions;

    public CombinedRetentionProvider(List<MetricRetention> configRetentions) {
        this.configRetentions = configRetentions;
        this.combinedRetentions = new ArrayList<>();
    }

    @Override
    public MetricRetention getRetention(String metric) {
        MetricRetention firstMatch = null;
        MetricRetention result;

        for (MetricRetention metricRetention : combinedRetentions) {
            if (metricRetention.matches(metric)) {
                return metricRetention;
            }
        }
        for (MetricRetention metricRetention : configRetentions) {
            if (metricRetention.getIsDefault()) {
                if (firstMatch == null) {
                    // There is only default match
                    if (metricRetention.getType() == MetricRetention.Type.ALL) {
                        return metricRetention;
                    }
                    break;
                } else if (firstMatch.getType() != metricRetention.getType()) {
                    // There is first partial retention pattern and default has a different type
                    if (firstMatch.getType() == MetricRetention.Type.RETENTION) {
                        result = makeCombinedRetention(firstMatch, metricRetention);
                        combinedRetentions.add(result);
                        return result;
                    }

                    if (firstMatch.getType() == MetricRetention.Type.AGGREGATION) {
                        result = makeCombinedRetention(metricRetention, firstMatch);
                        combinedRetentions.add(result);
                        return result;
                    }
                }

                break;
            } else if (metricRetention.matches(metric)) {
                if (metricRetention.getType() != MetricRetention.Type.ALL) {
                    // It's partial retention pattern
                    if (firstMatch == null) {
                        // And it's first match
                        firstMatch = metricRetention;
                        continue;
                    }

                    // It's second match and types are different
                    if (firstMatch.getType() == MetricRetention.Type.AGGREGATION
                        && metricRetention.getType() == MetricRetention.Type.RETENTION
                    ) {
                        result = makeCombinedRetention(metricRetention, firstMatch);
                        combinedRetentions.add(result);
                        return result;
                    }

                    if (firstMatch.getType() == MetricRetention.Type.RETENTION
                        && metricRetention.getType() == MetricRetention.Type.AGGREGATION
                    ) {
                        result = makeCombinedRetention(firstMatch, metricRetention);
                        combinedRetentions.add(result);
                        return result;
                    }
                } else {
                    // It's a typeAll retention pattern
                    return metricRetention;
                }
            }
        }
        throw new IllegalStateException("Retention for metric '" + metric + "' not found");
    }

    private MetricRetention makeCombinedRetention(MetricRetention retention, MetricRetention aggregation) {
        MetricRetention.MetricDataRetentionBuilder builder = MetricRetention.newBuilder(
            retention.getRegexp(),
            aggregation.getRegexp(),
            aggregation.getFunction()
        );

        return builder.build(retention.getRanges());
    }

}

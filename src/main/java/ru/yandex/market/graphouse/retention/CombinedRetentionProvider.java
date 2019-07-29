package ru.yandex.market.graphouse.retention;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Mikhail f. Shiryaev <a href="mailto:mr.felixoid@gmail.com"></a>
 * @date 07/03/2019
 */
public class CombinedRetentionProvider implements RetentionProvider {
    private final List<MetricRetention> configRetentions;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, MetricRetention>> combinedRetentions;

    public CombinedRetentionProvider(List<MetricRetention> configRetentions) {
        this.configRetentions = configRetentions;
        this.combinedRetentions = new ConcurrentHashMap<>();
    }

    @Override
    public MetricRetention getRetention(String metric) {
        MetricRetention firstMatch = null;
        MetricRetention result;

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
                        result = getOrMakeCombined(firstMatch, metricRetention);
                        return result;
                    }

                    if (firstMatch.getType() == MetricRetention.Type.AGGREGATION) {
                        result = getOrMakeCombined(metricRetention, firstMatch);
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
                        result = getOrMakeCombined(metricRetention, firstMatch);
                        return result;
                    }

                    if (firstMatch.getType() == MetricRetention.Type.RETENTION
                        && metricRetention.getType() == MetricRetention.Type.AGGREGATION
                    ) {
                        result = getOrMakeCombined(firstMatch, metricRetention);
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

    private MetricRetention getOrMakeCombined(MetricRetention retention, MetricRetention aggregation) {
        String rRegexp = retention.getRegexp();
        String aRegexp = aggregation.getRegexp();
        if (combinedRetentions.containsKey(rRegexp)) {
            ConcurrentHashMap<String, MetricRetention> subMap = combinedRetentions.get(rRegexp);
            if (subMap.containsKey(aRegexp)) {
                return subMap.get(aRegexp);
            } else {
                subMap.put(aRegexp,makeCombined(retention, aggregation));
                return subMap.get(aRegexp);
            }
        } else {
            ConcurrentHashMap<String, MetricRetention> subMap = new ConcurrentHashMap<>();
            combinedRetentions.put(rRegexp, subMap);
            subMap.put(aRegexp,makeCombined(retention, aggregation));
            return subMap.get(aRegexp);
        }
    }

    private MetricRetention makeCombined(MetricRetention retention, MetricRetention aggregation) {
        MetricRetention.MetricDataRetentionBuilder builder = MetricRetention.newBuilder(
            aggregation.getFunction()
        );

        return builder.build(retention.getRanges());
    }

}

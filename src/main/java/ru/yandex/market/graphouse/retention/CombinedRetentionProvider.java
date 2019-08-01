package ru.yandex.market.graphouse.retention;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Mikhail f. Shiryaev <a href="mailto:mr.felixoid@gmail.com"></a>
 * @date 07/03/2019
 */
public class CombinedRetentionProvider implements RetentionProvider {
    private final List<MetricRetentionConfig> configRetentions;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, MetricRetention>> combinedRetentions;

    public CombinedRetentionProvider(List<MetricRetentionConfig> configRetentions) {
        this.configRetentions = configRetentions;
        this.combinedRetentions = new ConcurrentHashMap<>();
    }

    @Override
    public MetricRetention getRetention(String metric) {
        MetricRetentionConfig firstMatch = null;
        MetricRetention result;

        for (MetricRetentionConfig metricRetentionConfig : configRetentions) {
            if (metricRetentionConfig.getIsDefault()) {
                if (firstMatch == null) {
                    // There is only default match
                    if (metricRetentionConfig.getType() == MetricRetentionConfig.Type.ALL) {
                        return metricRetentionConfig.getMetricRetention();
                    }
                    break;
                } else if (firstMatch.getType() != metricRetentionConfig.getType()) {
                    // There is first partial retention pattern and default has a different type
                    if (firstMatch.getType() == MetricRetentionConfig.Type.RETENTION) {
                        result = getOrMakeCombined(firstMatch, metricRetentionConfig);
                        return result;
                    }

                    if (firstMatch.getType() == MetricRetentionConfig.Type.AGGREGATION) {
                        result = getOrMakeCombined(metricRetentionConfig, firstMatch);
                        return result;
                    }
                }

                break;
            } else if (metricRetentionConfig.matches(metric)) {
                if (metricRetentionConfig.getType() != MetricRetentionConfig.Type.ALL) {
                    // It's partial retention pattern
                    if (firstMatch == null) {
                        // And it's first match
                        firstMatch = metricRetentionConfig;
                        continue;
                    }

                    // It's second match and types are different
                    if (firstMatch.getType() == MetricRetentionConfig.Type.AGGREGATION
                        && metricRetentionConfig.getType() == MetricRetentionConfig.Type.RETENTION
                    ) {
                        result = getOrMakeCombined(metricRetentionConfig, firstMatch);
                        return result;
                    }

                    if (firstMatch.getType() == MetricRetentionConfig.Type.RETENTION
                        && metricRetentionConfig.getType() == MetricRetentionConfig.Type.AGGREGATION
                    ) {
                        result = getOrMakeCombined(firstMatch, metricRetentionConfig);
                        return result;
                    }
                } else {
                    // It's a typeAll retention pattern
                    return metricRetentionConfig.getMetricRetention();
                }
            }
        }
        throw new IllegalStateException("Retention for metric '" + metric + "' not found");
    }

    private MetricRetention getOrMakeCombined(MetricRetentionConfig retention, MetricRetentionConfig aggregation) {
        String rRegexp = retention.getRegexp();
        String aRegexp = aggregation.getRegexp();
        if (combinedRetentions.containsKey(rRegexp)) {
            ConcurrentHashMap<String, MetricRetention> subMap = combinedRetentions.get(rRegexp);
            if (subMap.containsKey(aRegexp)) {
                return subMap.get(aRegexp);
            } else {
                subMap.put(aRegexp, makeCombined(retention, aggregation));
                return subMap.get(aRegexp);
            }
        } else {
            ConcurrentHashMap<String, MetricRetention> subMap = new ConcurrentHashMap<>();
            combinedRetentions.put(rRegexp, subMap);
            subMap.put(aRegexp,makeCombined(retention, aggregation));
            return subMap.get(aRegexp);
        }
    }

    private MetricRetention makeCombined(MetricRetentionConfig retention, MetricRetentionConfig aggregation) {
        MetricRetention.MetricDataRetentionBuilder builder = MetricRetention.newBuilder(
            aggregation.getMetricRetention().getFunction()
        );

        return builder.build(retention.getMetricRetention().getRanges());
    }

}

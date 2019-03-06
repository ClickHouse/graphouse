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
        MetricRetention first_match = null;

        for (MetricRetention metricRetention : combinedRetentions) {
            if (metricRetention.matches(metric)) {
                return metricRetention;
            }
        }
        for (MetricRetention metricRetention : configRetentions) {
            if (metricRetention.getIsDefault()) {
                // There is only default match
                if (first_match == null
                    && metricRetention.getType() == MetricRetention.typeAll
                ) {
                    return metricRetention;
                }
                // The default pattern contains necessary part for combination
                if (first_match.getType() != metricRetention.getType()) {
                    if (first_match.getType() == MetricRetention.typeRetention) {
                        combinedRetentions.add(
                            MetricRetention.newBuilder(
                                metricRetention.getMainPattern(),
                                first_match.getMainPattern(),
                                metricRetention.getFunction()
                            )
                                .build(first_match.getRanges())
                        );
                        return combinedRetentions.get(combinedRetentions.size() - 1);
                    }

                    if (first_match.getType() == MetricRetention.typeAggregation) {
                        combinedRetentions.add(
                            MetricRetention.newBuilder(
                                first_match.getMainPattern(),
                                metricRetention.getMainPattern(),
                                first_match.getFunction()
                            )
                                .build(first_match.getRanges())
                        );
                        return combinedRetentions.get(combinedRetentions.size() - 1);
                    }
                }

                break;
            } else if (metricRetention.matches(metric)) {
                if (metricRetention.getType() != MetricRetention.typeAll) {
                    if (first_match == null) {
                        first_match = metricRetention;
                        continue;
                    }

                    if (first_match.getType() == MetricRetention.typeAggregation
                        && metricRetention.getType() == MetricRetention.typeRetention
                    ) {
                        combinedRetentions.add(
                            MetricRetention.newBuilder(
                                first_match.getMainPattern(),
                                metricRetention.getMainPattern(),
                                first_match.getFunction()
                            )
                                .build(metricRetention.getRanges())
                        );
                        return combinedRetentions.get(combinedRetentions.size() - 1);
                    }

                    if (first_match.getType() == MetricRetention.typeRetention
                        && metricRetention.getType() == MetricRetention.typeAggregation
                    ) {
                        combinedRetentions.add(
                            MetricRetention.newBuilder(
                                metricRetention.getMainPattern(),
                                first_match.getMainPattern(),
                                metricRetention.getFunction()
                            )
                                .build(first_match.getRanges())
                        );
                        return combinedRetentions.get(combinedRetentions.size() - 1);
                    }
                } else {
                    return metricRetention;
                }
            }
        }
        throw new IllegalStateException("Retention for metric '" + metric + "' not found");
    }


}

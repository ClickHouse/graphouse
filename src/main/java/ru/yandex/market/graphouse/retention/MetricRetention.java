package ru.yandex.market.graphouse.retention;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 23.11.16
 */
public class MetricRetention {
    private final String function;
    private final RangeMap<Integer, Integer> ranges;


    private MetricRetention(String function) {
        this.function = function;
        this.ranges = TreeRangeMap.create();
    }

    @Override
    public String toString() {
        return "Function: " + function + "; Ranges: " + ranges.toString();
    }

    public String getFunction() {
        return function;
    }

    public RangeMap<Integer, Integer> getRanges() {
        return ranges;
    }

    public int getStepSize(int ageSeconds) {
        Integer step = ranges.get(Math.max(ageSeconds, 0));
        Preconditions.checkNotNull(
            step, "Could find retention step for age " + ageSeconds + ", values: " + ranges.toString()
        );
        return step;
    }

    public static MetricDataRetentionBuilder newBuilder(String function) {
        return new MetricDataRetentionBuilder(function);
    }

    public static class MetricDataRetentionBuilder {
        private Map<Integer, Integer> ageRetentionMap = new HashMap<>();
        private final MetricRetention result;

        public MetricDataRetentionBuilder(String function) {
            result = new MetricRetention(function);
        }

        public MetricRetention build() {
            refillRetentions();
            return result;
        }

        public MetricRetention build(RangeMap<Integer, Integer> ranges) {
            result.ranges.clear();
            result.ranges.putAll(ranges);
            return result;
        }

        public MetricDataRetentionBuilder addRetention(int age, int retention) {
            if (age == 0 && retention == 0) {
                ageRetentionMap = null;
            } else {
                ageRetentionMap.put(age, retention);
            }
            return this;
        }

        private void refillRetentions() {
            result.ranges.clear();

            if (ageRetentionMap == null) {
                return;
            }

            int counter = 0;
            final int valuesMaxIndex = ageRetentionMap.values().size() - 1;
            final List<Map.Entry<Integer, Integer>> entryList = ageRetentionMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toList());

            for (Map.Entry<Integer, Integer> retention : entryList) {
                final Integer age = retention.getKey();
                final Integer precision = retention.getValue();

                final boolean isLast = (counter == valuesMaxIndex);

                if (!isLast) {
                    final Integer nextAge = entryList.get(counter + 1).getKey();
                    result.ranges.put(Range.closedOpen(age, nextAge), precision);
                } else {
                    result.ranges.put(Range.atLeast(age), precision);
                }
                counter++;
            }
        }
    }
}

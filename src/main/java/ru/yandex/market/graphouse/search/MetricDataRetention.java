package ru.yandex.market.graphouse.search;

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
public class MetricDataRetention {
    private final Pattern pattern;
    private final String function;
    private final RangeMap<Integer, Integer> ranges;

    private MetricDataRetention(Pattern pattern, String function) {
        this.pattern = pattern;
        this.function = function;
        this.ranges = TreeRangeMap.create();
    }

    public String getFunction() {
        return function;
    }

    boolean validateName(String name) {
        return pattern.matcher(name).matches();
    }

    public Integer getStepSize(int endTimeSeconds) {
        return ranges.get(endTimeSeconds);
    }

    public static class MetricDataRetentionBuilder {
        private final Map<Integer, Integer> ageRetentionMap = new HashMap<>();
        private final MetricDataRetention result;

        public MetricDataRetentionBuilder(String pattern, String function) {
            result = new MetricDataRetention(Pattern.compile(pattern), function);
        }

        public MetricDataRetention build() {
            refillRetentions();
            return result;
        }

        public MetricDataRetentionBuilder addRetention(int age, int retention) {
            ageRetentionMap.put(age, retention);
            return this;
        }

        private void refillRetentions() {
            result.ranges.clear();

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

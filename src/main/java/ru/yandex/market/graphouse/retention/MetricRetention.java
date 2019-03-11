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
    private final Pattern regexp;
    private final Pattern combinedRegexp;
    private final String function;
    private final boolean isDefault;
    private final RangeMap<Integer, Integer> ranges;
    public final static int typeRetention = 1;
    public final static int typeAggregation = 2;
    public final static int typeAll = 3;

    private MetricRetention(Pattern regexp, String function, boolean isDefault) {
        this.regexp = regexp;
        this.combinedRegexp = null;
        this.function = function;
        this.isDefault = isDefault;
        this.ranges = TreeRangeMap.create();
    }

    private MetricRetention(Pattern regexp, Pattern combinedRegexp, String function) {
        this.regexp = regexp;
        this.combinedRegexp = combinedRegexp;
        this.function = function;
        this.isDefault = false;
        this.ranges = TreeRangeMap.create();
    }

    @Override
    public String toString() {
        return "Main regexp: " + regexp + "; Second pattern: " + combinedRegexp + "; Function: " + function +
            "; Ranges: " + ranges.toString();
    }

    public String getFunction() {
        return function;
    }

    public String getRegexp() { return regexp.toString(); }

    public RangeMap<Integer, Integer> getRanges() { return ranges; }

    public boolean getIsDefault() { return isDefault; }

    public int getType() {
        if (function.equals("")) {
            return typeRetention;
        }
        if (ranges.asMapOfRanges().isEmpty()) {
            return typeAggregation;
        }
        return typeAll;
    }

    boolean matches(String name) {
        if (isDefault) {
            return true;
        }

        if (combinedRegexp == null) {
            return regexp.matcher(name).matches();
        }

        return regexp.matcher(name).matches() && combinedRegexp.matcher(name).matches();
    }

    public int getStepSize(int ageSeconds) {
        Integer step = ranges.get(Math.max(ageSeconds, 0));
        Preconditions.checkNotNull(
            step, "Could find retention step for age " + ageSeconds + ", values: " + ranges.toString()
        );
        return step;
    }

    public static MetricDataRetentionBuilder newBuilder(String mainPattern, String function, boolean isDefault) {
        return new MetricDataRetentionBuilder(mainPattern, function, isDefault);
    }

    public static MetricDataRetentionBuilder newBuilder(String mainPattern, String secondPattern, String function) {
        return new MetricDataRetentionBuilder(mainPattern, secondPattern, function);
    }

    public static class MetricDataRetentionBuilder {
        private Map<Integer, Integer> ageRetentionMap = new HashMap<>();
        private final MetricRetention result;

        public MetricDataRetentionBuilder(String mainPattern, String function, boolean isDefault) {
            result = new MetricRetention(Pattern.compile(mainPattern), function, isDefault);
        }

        public MetricDataRetentionBuilder(String mainPattern, String secondPattern, String function) {
            result = new MetricRetention(Pattern.compile(mainPattern), Pattern.compile(secondPattern), function);
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
            if (age == 0 && retention ==0 ) {
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

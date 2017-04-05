package ru.yandex.market.graphouse.utils;

import ru.yandex.market.graphouse.search.tree.MetricDescription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 07.11.16
 */
public class AppendableList implements AppendableResult {

    private final List<MetricDescription> data = new ArrayList<>();

    @Override
    public void appendMetric(MetricDescription metric) throws IOException {
        data.add(metric);
    }

    public List<MetricDescription> getSortedList() {
        return data.stream()
            .sorted(Comparator.comparing(MetricDescription::getName))
            .collect(Collectors.toList());
    }


    public List<MetricDescription> getList() {
        return data;
    }
}
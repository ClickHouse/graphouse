package ru.yandex.market.graphouse.search.tree;

import ru.yandex.market.graphouse.search.MetricStatus;

import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 02/09/15
 */
public interface MetricDescription {

    String getName();

    int getNameLength();

    void writeName(DataOutput out) throws IOException;

    MetricStatus getStatus();

    boolean isDir();

    long getUpdateTimeMillis();

    MetricDescription getParent();

    int getLevel();

    boolean isRoot();

}

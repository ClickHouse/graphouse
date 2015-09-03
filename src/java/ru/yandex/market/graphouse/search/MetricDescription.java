package ru.yandex.market.graphouse.search;

import org.apache.http.util.ByteArrayBuffer;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 02/09/15
 */
public interface MetricDescription {
    void writeName(ByteArrayBuffer buffer);

    String getName();

    MetricStatus getStatus();

    boolean isDir();
}

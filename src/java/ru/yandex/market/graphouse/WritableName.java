package ru.yandex.market.graphouse;

import org.apache.http.util.ByteArrayBuffer;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 02/09/15
 */
public interface WritableName {
    void writeName(ByteArrayBuffer buffer);
}

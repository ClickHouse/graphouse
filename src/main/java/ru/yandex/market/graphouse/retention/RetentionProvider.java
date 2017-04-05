package ru.yandex.market.graphouse.retention;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 03/04/2017
 */
public interface RetentionProvider {

    MetricRetention getRetention(String metric);

}

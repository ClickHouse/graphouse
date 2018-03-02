package ru.yandex.market.graphouse.render;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 13/02/2017
 */
public class RequestParseException extends Exception {
    public RequestParseException(String error, String pattern) {
        super(error + " " + pattern); //TODO
    }
}

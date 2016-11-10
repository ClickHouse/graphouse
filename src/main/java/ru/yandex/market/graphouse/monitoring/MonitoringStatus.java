package ru.yandex.market.graphouse.monitoring;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 10.11.16
 */
public enum MonitoringStatus {
    OK(0),
    WARNING(1),
    CRITICAL(2);

    private final int code;

    MonitoringStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}

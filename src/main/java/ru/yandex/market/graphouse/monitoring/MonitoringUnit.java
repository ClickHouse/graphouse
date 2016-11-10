package ru.yandex.market.graphouse.monitoring;

import java.util.Objects;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 10.11.16
 */
public class MonitoringUnit {
    private final String name;
    private volatile MonitoringStatus status = MonitoringStatus.OK;
    private volatile String message;
    private volatile Throwable exception;

    public MonitoringUnit(String name) {
        this.name = name;
    }

    public void ok() {
        update(MonitoringStatus.OK, null, null);
    }

    public void warning(String message) {
        warning(message, null);
    }

    public void warning(Throwable exception) {
        warning(exception.getMessage(), exception);
    }

    public void warning(String message, Throwable exception) {
        update(MonitoringStatus.WARNING, message, exception);
    }

    public void critical(String message) {
        critical(message, null);
    }

    public void critical(Throwable exception) {
        critical(exception.getMessage(), exception);
    }

    public void critical(String message, Throwable exception) {
        update(MonitoringStatus.CRITICAL, message, exception);
    }

    private synchronized void update(MonitoringStatus status, String message, Throwable exception) {
        this.status = status;
        this.message = message;
        this.exception = exception;
    }

    public String getName() {
        return name;
    }

    public MonitoringStatus getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public Throwable getException() {
        return exception;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MonitoringUnit that = (MonitoringUnit) o;
        return Objects.equals(name, that.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}

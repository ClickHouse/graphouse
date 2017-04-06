package ru.yandex.market.graphouse.monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 20/04/15
 */
public class MonitoringUnit {
    private static final Logger log = LoggerFactory.getLogger(MonitoringUnit.class);

    protected final String name;
    protected volatile String message;
    protected volatile long monitoringDelayMillis = -1;

    protected volatile long problemStartTimeMillis = -1;
    protected volatile Throwable exception;
    protected volatile MonitoringStatus status = MonitoringStatus.OK;
    protected volatile boolean logEnabled;

    public MonitoringUnit(String name) {
        this.name = name;
    }

    public MonitoringUnit(String name, int delay, TimeUnit timeUnit) {
        this.name = name;
        setMonitoringDelay(delay, timeUnit);
    }

    public void ok() {
        update(MonitoringStatus.OK, null, null);
    }

    public void ok(String message) {
        update(MonitoringStatus.OK, message, null);
    }

    public void warning(String message) {
        warning(message, null);
    }

    public void warning(Throwable exception) {
        warning(exception.getMessage(), exception);
    }

    public void warning(String message, Throwable exception) {
        if (logEnabled) {
            log.warn(message, exception);
        }
        update(MonitoringStatus.WARNING, message, exception);
    }

    public void critical(String message) {
        critical(message, null);
    }

    public void critical(Throwable exception) {
        critical(exception.getMessage(), exception);
    }

    public void critical(String message, Throwable exception) {
        if (logEnabled) {
            log.error(message, exception);
        }
        update(MonitoringStatus.CRITICAL, message, exception);
    }

    synchronized void update(MonitoringStatus status, String message, Throwable exception) {
        setProblemStartTimeMillis(status);
        this.status = status;
        this.message = message;
        this.exception = exception;
    }

    void setProblemStartTimeMillis(MonitoringStatus status) {
        if (this.status != status) {
            if (status == MonitoringStatus.OK) {
                problemStartTimeMillis = -1;
            } else {
                problemStartTimeMillis = System.currentTimeMillis();
            }
        }
    }

    public String getName() {
        return name;
    }

    public MonitoringStatus getStatus() {
        return isDelayActive() ? MonitoringStatus.OK : status;
    }

    private boolean isDelayActive() {
        return (System.currentTimeMillis() - problemStartTimeMillis) <= monitoringDelayMillis;
    }

    public String getMessage() {
        return message;
    }

    public Throwable getException() {
        return exception;
    }

    public synchronized void disableMonitoringDelay() {
        monitoringDelayMillis = -1;
    }

    public synchronized void setMonitoringDelay(int delay, TimeUnit timeUnit) {
        monitoringDelayMillis = timeUnit.toMillis(delay);
    }

    public void setLogEnabled(boolean logEnabled) {
        this.logEnabled = logEnabled;
    }

    @Override
    public String toString() {
        return "MonitoringUnit{" +
            "message='" + message + '\'' +
            ", exception=" + exception +
            ", status=" + status +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MonitoringUnit that = (MonitoringUnit) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

}

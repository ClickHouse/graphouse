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
    private volatile long lastUpdateMillis;
    private volatile long criticalTimeoutMillis = -1;
    private volatile long warningTimeoutMillis = -1;

    public MonitoringUnit(String name) {
        this.name = name;
        this.lastUpdateMillis = System.currentTimeMillis();
    }

    public MonitoringUnit(String name, int delay, TimeUnit timeUnit) {
        this(name);
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

    protected synchronized void update(MonitoringStatus status, String message, Throwable exception) {
        setProblemStartTimeMillis(status);
        this.status = status;
        this.message = message;
        this.exception = exception;
        setLastUpdateMillis(System.currentTimeMillis());
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

    protected void setLastUpdateMillis(long lastUpdateMillis) {
        this.lastUpdateMillis = lastUpdateMillis;
    }

    protected long getLastUpdateMillis() {
        return lastUpdateMillis;
    }

    public MonitoringStatus getStatus() {
        final boolean isDelayActive = (System.currentTimeMillis() - problemStartTimeMillis) < monitoringDelayMillis;
        if (isDelayActive) {
            return MonitoringStatus.OK;
        }

        final long millisFromLastUpdate = System.currentTimeMillis() - getLastUpdateMillis();
        if (criticalTimeoutMillis > 0 && millisFromLastUpdate > criticalTimeoutMillis) {
            message = "Critical execution time exceeded";
            return MonitoringStatus.CRITICAL;
        } else if (warningTimeoutMillis > 0 && millisFromLastUpdate > warningTimeoutMillis) {
            message = "Execution time exceeded";
            return MonitoringStatus.WARNING;
        }

        return status;
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

    public void setCriticalTimeoutMillis(long criticalTimeoutMillis) {
        this.criticalTimeoutMillis = criticalTimeoutMillis;
    }

    public void setCriticalTimeout(long criticalTimeout, TimeUnit timeUnit) {
        this.criticalTimeoutMillis = timeUnit.toMillis(criticalTimeout);
    }

    public void setWarningTimeoutMillis(long warningTimeoutMillis) {
        this.warningTimeoutMillis = warningTimeoutMillis;
    }

    public void setWarningTimeout(long warningTimeout, TimeUnit timeUnit) {
        this.warningTimeoutMillis = timeUnit.toMillis(warningTimeout);
    }
}

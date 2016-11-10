package ru.yandex.market.graphouse.monitoring;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 10.11.16
 */
public class Monitoring {

    private static final String OK_TEXT = "OK";
    private static final String WARNING_TEXT = "WARN";
    private static final String CRITICAL_TEXT = "CRIT";
    private static final Result OK_RESULT = new Result(MonitoringStatus.OK, OK_TEXT);

    private final Map<String, UnitWrapper> units = new ConcurrentHashMap<>();

    private volatile Result result;

    public Result getResult() {
        updateResult();
        return result;
    }

    public void addUnit(MonitoringUnit unit) {
        units.put(unit.getName(), new UnitWrapper(unit));
    }

    public void addTemporaryCritical(String name, String message, long time, TimeUnit timeUnit) {
        addTemporary(name, MonitoringStatus.CRITICAL, message, time, timeUnit);
    }

    public void addTemporaryWarning(String name, String message, long time, TimeUnit timeUnit) {
        addTemporary(name, MonitoringStatus.WARNING, message, time, timeUnit);
    }

    private void addTemporary(String name, MonitoringStatus status, String message, long time, TimeUnit timeUnit) {
        MonitoringUnit unit = new MonitoringUnit(name);
        switch (status) {
            case WARNING:
                unit.warning(message);
                break;
            case CRITICAL:
                unit.critical(message);
                break;
        }
        long validTillMillis = System.currentTimeMillis() + timeUnit.toMillis(time);
        addTemporaryUntil(unit, validTillMillis);
    }

    public void addTemporary(MonitoringUnit unit, long time, TimeUnit timeUnit) {
        long validTillMillis = System.currentTimeMillis() + timeUnit.toMillis(time);
        addTemporaryUntil(unit, validTillMillis);
    }

    public void addTemporaryUntil(MonitoringUnit unit, long validTillMillis) {
        if (unit.getStatus().equals(MonitoringStatus.OK)) {
            units.remove(unit.getName());
        } else {
            units.put(unit.getName(), new UnitWrapper(unit, validTillMillis));
        }
    }

    public void addTemporaryUntil(MonitoringUnit unit, Date date) {
        addTemporaryUntil(unit, date.getTime());
    }

    private void updateResult() {
        StringBuilder warningBuilder = new StringBuilder();
        StringBuilder criticalBuilder = new StringBuilder();

        Iterator<Map.Entry<String, UnitWrapper>> unitIterator = units.entrySet().iterator();
        while (unitIterator.hasNext()) {
            Map.Entry<String, UnitWrapper> unitEntry = unitIterator.next();
            if (!unitEntry.getValue().isValid()) {
                unitIterator.remove();
                continue;
            }
            final MonitoringUnit unit = unitEntry.getValue().unit;
            synchronized (unit) {
                switch (unit.getStatus()) {
                    case OK:
                        continue;
                    case WARNING:
                        appendToMessage(warningBuilder, unit);
                        break;
                    case CRITICAL:
                        appendToMessage(criticalBuilder, unit);
                        break;
                }
            }
        }
        StringBuilder message = new StringBuilder();
        MonitoringStatus status = MonitoringStatus.OK;
        if (criticalBuilder.length() > 0) {
            message.append(CRITICAL_TEXT).append(" {").append(criticalBuilder).append("}");
            status = MonitoringStatus.CRITICAL;
        }
        if (warningBuilder.length() > 0) {
            if (message.length() > 0) {
                message.append(" ");
            } else {
                status = MonitoringStatus.WARNING;
            }
            message.append(WARNING_TEXT).append(" {").append(warningBuilder).append("}");
        }
        if (message.length() > 0) {
            result = new Result(status, message.toString());
        } else {
            result = OK_RESULT;
        }
    }

    private void appendToMessage(StringBuilder message, MonitoringUnit unit) {
        if (message.length() > 0) {
            message.append(", ");
        }
        message.append(unit.getName()).append(": ").append(unit.getMessage());
    }

    public static class Result {
        private final MonitoringStatus status;
        private final String message;

        public Result(MonitoringStatus status, String message) {
            this.status = status;
            this.message = message;
        }

        public MonitoringStatus getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return status.getCode() + ";" + message;
        }
    }

    private static class UnitWrapper {
        private final MonitoringUnit unit;
        private final long validTillMillis;

        public UnitWrapper(MonitoringUnit unit, long validTillMillis) {
            this.unit = unit;
            this.validTillMillis = validTillMillis;
        }

        public UnitWrapper(MonitoringUnit unit) {
            this(unit, -1);
        }

        private boolean isValid() {
            if (validTillMillis < 0) {
                return true;
            }
            return System.currentTimeMillis() <= validTillMillis;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UnitWrapper that = (UnitWrapper) o;
            return Objects.equals(unit, that.unit);
        }

        @Override
        public int hashCode() {
            return Objects.hash(unit);
        }
    }
}

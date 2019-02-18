package ru.yandex.market.graphouse.monitoring;

import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 2019-02-15
 */
public class BalancedClickhouseDataSourceMonitoring extends AbstractScheduledService {

    private static final Logger log = LogManager.getLogger();

    private final BalancedClickhouseDataSource dataSource;
    private final MonitoringUnit monitoringUnit;
    private final MonitoringUnit pingUnit;
    private final int clickHousePingRateSeconds;
    private final int totalServers;

    public BalancedClickhouseDataSourceMonitoring(BalancedClickhouseDataSource dataSource,
                                                  Monitoring monitoring, Monitoring ping,
                                                  int clickHousePingRateSeconds) {
        this.dataSource = dataSource;
        this.clickHousePingRateSeconds = clickHousePingRateSeconds;
        this.totalServers = dataSource.getAllClickhouseUrls().size();

        monitoringUnit = new MonitoringUnit("clickhouse");
        monitoring.addUnit(monitoringUnit);
        pingUnit = new MonitoringUnit("clickhouse");
        ping.addUnit(pingUnit);
    }

    @Override
    protected void runOneIteration() throws Exception {
        int available = dataSource.actualize();
        if (available == totalServers) {
            pingUnit.ok();
            monitoringUnit.ok();
            return;
        }
        log.warn("Unavailable clickhouse servers: " + dataSource.getDisabledUrls());
        if (available == 0) {
            pingUnit.critical("All clickhouse servers unavailable");
        }
        String message = String.format("Only %d of %d servers available", available, totalServers);
        if (available < totalServers / 2.) {
            monitoringUnit.critical(message);
        } else {
            monitoringUnit.warning(message);
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(clickHousePingRateSeconds, clickHousePingRateSeconds, TimeUnit.SECONDS);
    }
}

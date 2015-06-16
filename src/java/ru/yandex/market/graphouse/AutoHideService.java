package ru.yandex.market.graphouse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import ru.yandex.market.clickhouse.ClickhouseTemplate;
import ru.yandex.market.clickhouse.HttpResultRow;
import ru.yandex.market.clickhouse.HttpRowCallbackHandler;
import ru.yandex.market.graphouse.search.MetricSearch;
import ru.yandex.market.graphouse.search.MetricStatus;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 11/06/15
 */
public class AutoHideService implements InitializingBean, Runnable {

    private static final Logger log = LogManager.getLogger();
    private static final int BATCH_SIZE = 5_000;

    private ClickhouseTemplate clickhouseTemplate;
    private MetricSearch metricSearch;
    private boolean enabled = true;

    private int maxValuesCount = 200;
    private int missingDays = 7;
    private int runDelayMinutes = 10;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void afterPropertiesSet() throws Exception {
        if (!enabled) {
            log.info("Autohide disabled");
            return;
        }
        scheduler.scheduleAtFixedRate(this, runDelayMinutes, TimeUnit.DAYS.toMinutes(1), TimeUnit.MINUTES);
        log.info("Autohide scheduled");
    }

    @Override
    public void run() {
        hide();
    }

    private void hide() {
        log.info("Running autohide.");
        final AtomicInteger count = new AtomicInteger();
        final List<String> metrics = new ArrayList<>(BATCH_SIZE);
        try {
            clickhouseTemplate.query(
                "select Path, count() as cnt, max(Timestamp) as ts from graphite group by Path " +
                    "having cnt < " + maxValuesCount + " and ts < toUInt32(toDateTime(today() - " + missingDays + "))",
                new HttpRowCallbackHandler() {
                    @Override
                    public void processRow(HttpResultRow rs) throws SQLException {
                        String metric = rs.getString(1);
                        metrics.add(metric);
                        count.incrementAndGet();
                        if (metrics.size() >= BATCH_SIZE) {
                            metricSearch.modify(metrics, MetricStatus.AUTO_HIDDEN);
                            metrics.clear();
                            log.info(count.get() + " metrics hidden");
                        }
                    }
                }
            );
            metricSearch.modify(metrics, MetricStatus.AUTO_HIDDEN);
            log.info("Autohide completed. Total" + count.get() + " metrics hidden");
        } catch (Exception e) {
            log.error("Failed to run autohide.", e);
        }
    }

    public void setRunDelayMinutes(int runDelayMinutes) {
        this.runDelayMinutes = runDelayMinutes;
    }

    public void setMaxValuesCount(int maxValuesCount) {
        this.maxValuesCount = maxValuesCount;
    }

    public void setMissingDays(int missingDays) {
        this.missingDays = missingDays;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Required
    public void setMetricSearch(MetricSearch metricSearch) {
        this.metricSearch = metricSearch;
    }

    @Required
    public void setClickhouseTemplate(ClickhouseTemplate clickhouseTemplate) {
        this.clickhouseTemplate = clickhouseTemplate;
    }
}

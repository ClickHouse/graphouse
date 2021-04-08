package ru.yandex.market.graphouse.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.util.concurrent.TimeUnit;

public class ClickHouseQueryExecutor {
    private static final Logger log = LogManager.getLogger();

    private final JdbcTemplate clickHouseJdbcTemplate;
    private final int queryRetryCount;
    private final int queryRetryIncrementSec;

    public ClickHouseQueryExecutor(
        JdbcTemplate clickHouseJdbcTemplate, int queryRetryCount, int queryRetryIncrementSec
    ) {
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.queryRetryCount = queryRetryCount;
        this.queryRetryIncrementSec = queryRetryIncrementSec;
    }

    public void executeQuery(String sql, RowCallbackHandler handler, Object... args) {
        int attempts = 0;
        int waitTime = 1;
        while (!tryExecuteQuery(sql, handler, args)) {
            try {
                attempts++;
                if (attempts >= queryRetryCount) {
                    log.error("Can't execute query: \"{}\" with arguments {}", sql, args);
                    break;
                } else {
                    log.warn(
                        "Failed to execute query. Attempt number {} / {}. Waiting {} second before retry",
                        attempts, queryRetryCount, waitTime
                    );
                    TimeUnit.SECONDS.sleep(waitTime);
                    waitTime += queryRetryIncrementSec;
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private boolean tryExecuteQuery(String sql, RowCallbackHandler handler, Object... args) {
        try {
            clickHouseJdbcTemplate.query(sql, handler, args);
        } catch (RuntimeException e) {
            log.error("Failed to execute query", e);
            return false;
        }
        return true;
    }
}

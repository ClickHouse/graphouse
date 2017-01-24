package ru.yandex.market.graphouse.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.market.graphouse.AutoHideService;
import ru.yandex.market.graphouse.search.MetricSearch;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 10.11.16
 */
@Configuration
public class AutohideConfig {

    @Value("${graphouse.autohide.enabled}")
    private boolean autohideEnabled;

    @Value("${graphouse.autohide.run-delay-minutes}")
    private int autohideRunDelayMinutes;

    @Value("${graphouse.autohide.max-values-count}")
    private int autohideMaxValues;

    @Value("${graphouse.autohide.missing-days}")
    private int autohideMissingDays;

    @Value("${graphouse.autohide.step}")
    private int autohideStepSize;

    @Value("${graphouse.autohide.retry.count}")
    private int autohideRetryCount;

    @Value("${graphouse.autohide.retry.wait_seconds}")
    private int autohideRetryWaitSeconds;

    @Value("${graphite.metric.data.table}")
    private String metricDataTable;

    @Autowired
    private JdbcTemplate clickHouseJdbcTemplate;

    @Autowired
    private MetricSearch metricSearch;

    @Bean
    public AutoHideService autoHideService() {
        final AutoHideService autoHideService =
            new AutoHideService(clickHouseJdbcTemplate, metricSearch, metricDataTable);
        autoHideService.setEnabled(autohideEnabled);
        autoHideService.setRunDelayMinutes(autohideRunDelayMinutes);
        autoHideService.setMaxValuesCount(autohideMaxValues);
        autoHideService.setMissingDays(autohideMissingDays);
        autoHideService.setStepSize(autohideStepSize);
        autoHideService.setRetryCount(autohideRetryCount);
        autoHideService.setRetryWaitSeconds(autohideRetryWaitSeconds);

        return autoHideService;
    }
}

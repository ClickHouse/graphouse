package ru.yandex.market.graphouse.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 04/04/2017
 */
public class ClickHouseRetentionProvider extends CombinedRetentionProvider {

    private static final Logger log = LogManager.getLogger();

    public ClickHouseRetentionProvider(JdbcTemplate clickHouseJdbcTemplate, String configName) {
        super(loadRetentions(clickHouseJdbcTemplate, configName));
    }

    private static List<MetricRetention> loadRetentions(JdbcTemplate jdbcTemplate, String configName) {

        log.info("Loading retentions from ClickHouse, config: " + configName);

        List<MetricRetention> retentions = jdbcTemplate.query(
            "SELECT priority, is_default, regexp, function, " +
                "groupArray(age) AS ages, groupArray(precision) AS precisions FROM (" +
                "   SELECT * FROM system.graphite_retentions WHERE config_name = ? ORDER BY priority, age" +
                ") GROUP BY regexp, function, priority, is_default ORDER BY priority",
            (rs, rowNum) -> {
                String pattern = ".*" + rs.getString("regexp") + ".*";
                String function = rs.getString("function");
                boolean isDefault = rs.getInt("is_default") == 1;
                MetricRetention.MetricDataRetentionBuilder builder = MetricRetention.newBuilder(pattern, function, isDefault);
                int[] ages = getIntArray(rs.getString("ages"));
                int[] precisions = getIntArray(rs.getString("precisions"));
                for (int i = 0; i < ages.length; i++) {
                    builder.addRetention(ages[i], precisions[i]);
                }
                return builder.build();
            },
            configName
        );
        if (retentions.isEmpty()) {
            throw new IllegalStateException("No retentions found in ClickHouse for config: " + configName);
        }
        log.info("Loaded " + retentions.size() + " retentions");
        return retentions;
    }

    //TODO move to CH jdbc driver
    private static int[] getIntArray(String string) {
        if (string.equals("[]")) {
            return new int[]{};
        }
        string = string.substring(1, string.length() - 1);
        String[] splits = string.split(",");
        int[] array = new int[splits.length];
        for (int i = 0; i < splits.length; i++) {
            array[i] = Integer.parseInt(splits[i]);
        }
        return array;
    }

}

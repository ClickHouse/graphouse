package ru.yandex.market.graphouse.render;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.market.graphouse.config.MetricsConfig;
import ru.yandex.market.graphouse.config.ServerConfig;
import ru.yandex.market.graphouse.monitoring.Monitoring;
import ru.yandex.market.graphouse.search.MetricSearch;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static java.lang.Double.NaN;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 04/11/2017
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RenderServiceTest.Config.class)
public class RenderServiceTest {

    public static int CLICKHOUSE_HTTP_PORT = 8123;

    @ClassRule
    public static GenericContainer clickhouse =
        new GenericContainer("yandex/clickhouse-server:latest")
            .withExposedPorts(CLICKHOUSE_HTTP_PORT)
            .withFileSystemBind(
                "src/test/data/etc-clickhouse-conf", "/etc/clickhouse-server/conf.d", BindMode.READ_ONLY
            );

    @Autowired
    private RenderService renderService;

    @Autowired
    private MetricSearch metricSearch;

    @Before
    public void setUp() throws Exception {
        metricSearch.loadNewMetrics();
        Preconditions.checkState(metricSearch.isMetricTreeLoaded());
    }

    @Test
    public void testSingleMetric() throws Exception {
        newTestBuilder("dir1.metric1")
            .withTimestamps(1, 5)
            .withExpectedTarget("dir1.metric1")
            .withDataPoints(1, 1, 1, 2, 3, 4, 5)
            .runTest();

        newTestBuilder("dir2.metric1")
            .withTimestamps(1, 5)
            .withExpectedTarget("dir2.metric1")
            .withDataPoints(1, 1, 1, NaN, 3, NaN, 5)
            .runTest();
    }

    @Test
    public void testMultipleMetrics() throws Exception {
        newTestBuilder("dir[12].metric1")
            .withTimestamps(1, 5)
            .withExpectedTarget("dir1.metric1")
            .withDataPoints(1, 1, 1, 2, 3, 4, 5)
            .withExpectedTarget("dir2.metric1")
            .withDataPoints(1, 1, 1, NaN, 3, NaN, 5)
            .runTest();
    }

    @Test
    public void testEmpty() throws Exception {
        newTestBuilder("some.unexisting.metric")
            .withTimestamps(1, 5)
            .runTest();
    }

    @Test
    public void testAvg() throws Exception {
        newTestBuilder("avg(dir[12].metric1)")
            .withTimestamps(1, 5)
            .withExpectedTarget("averageSeries(dir[12].metric1)")
            .withDataPoints(1, 1, 1, 2, 3, 4, 5)
            .runTest();

        newTestBuilder("avgSeries(dir2.metric[1223])")
            .withTimestamps(1, 5)
            .withExpectedTarget("averageSeries(dir2.metric[1223])")
            .withDataPoints(1, 1, 1.5, 4, 5.5, 8, 5)
            .runTest();

        newTestBuilder("average(dir2.metric[13])")
            .withTimestamps(3, 5)
            .withExpectedTarget("averageSeries(dir2.metric[13])")
            .withDataPoints(3, 1, 5.5, NaN, 5)
            .runTest();
    }


    private TestBuilder newTestBuilder(String target) {
        return new TestBuilder(target);
    }

    private class TestBuilder {
        private final String target;
        private int startTimestampSeconds;
        private int endTimestampSeconds;
        private int maxDataPoints = -1;
        private Map<String, SortedMap<Integer, Double>> expectedResults = new HashMap<>();

        public TestBuilder(String target) {
            this.target = target;
        }

        public TestBuilder withTimestamps(int startTimestampSeconds, int endTimestampSeconds) {
            this.startTimestampSeconds = startTimestampSeconds;
            this.endTimestampSeconds = endTimestampSeconds;
            return this;
        }

        public TestBuilder withMaxDataPoints(int maxDataPoints) {
            this.maxDataPoints = maxDataPoints;
            return this;
        }

        public ExpectedTargetBuilder withExpectedTarget(String expectedTarget) {
            return new ExpectedTargetBuilder(this, expectedTarget);
        }

        public void runTest() throws Exception {
            StringWriter resultWriter = new StringWriter();
            renderService.render(
                startTimestampSeconds, endTimestampSeconds, target, maxDataPoints, new PrintWriter(resultWriter)
            );
            JsonArray resultObject = new Gson().fromJson(resultWriter.toString(), JsonArray.class);
            SortedSet<String> expectedMetrics = new TreeSet<>(expectedResults.keySet());
            for (JsonElement metricElement : resultObject) {
                JsonObject metricObject = metricElement.getAsJsonObject();
                String target = metricObject.getAsJsonPrimitive("target").getAsString();
                SortedMap<Integer, Double> expected = expectedResults.remove(target);
                Preconditions.checkState(
                    expected != null,
                    "Found unexpected target in result '%s', expected on of: %s", target, expectedMetrics
                );
                SortedMap<Integer, Double> actual = parseDatapoints(metricObject.getAsJsonArray("datapoints"));
                Assert.assertEquals(target, expected, actual);
            }
            Preconditions.checkState(expectedResults.isEmpty(), "Targets not found in result: %s", expectedResults.keySet());
        }
    }

    private SortedMap<Integer, Double> parseDatapoints(JsonArray datapointsArray) {
        SortedMap<Integer, Double> actual = new TreeMap<>();
        for (JsonElement element : datapointsArray) {
            JsonArray valueArray = element.getAsJsonArray();
            int timestampSeconds = valueArray.get(1).getAsInt();
            JsonElement valueElement = valueArray.get(0);
            double value = valueElement.isJsonNull() ? NaN : valueElement.getAsDouble();
            Double prevValue = actual.put(timestampSeconds, value);
            Preconditions.checkState(prevValue == null);
        }
        return actual;
    }

    private static class ExpectedTargetBuilder {
        private final TestBuilder testBuilder;
        private final String target;
        private final SortedMap<Integer, Double> values = new TreeMap<>();

        public ExpectedTargetBuilder(TestBuilder testBuilder, String target) {
            this.testBuilder = testBuilder;
            this.target = target;
            testBuilder.expectedResults.put(target, values);
        }

        public ExpectedTargetBuilder withDataPoint(int timestampSeconds, double value) {
            values.put(timestampSeconds, value);
            return this;
        }

        public ExpectedTargetBuilder withDataPoints(int fromTimestampSeconds, int stepSeconds, double... values) {
            for (int i = 0; i < values.length; i++) {
                this.values.put(fromTimestampSeconds + (i * stepSeconds), values[i]);

            }
            return this;
        }

        public ExpectedTargetBuilder withExpectedTarget(String expectedTarget) {
            return new ExpectedTargetBuilder(testBuilder, expectedTarget);
        }

        public void runTest() throws Exception {
            testBuilder.runTest();
        }

    }

    public static void prepareTestData(JdbcTemplate jdbcTemplate) {

        jdbcTemplate.update(
            "CREATE TABLE default.data\n" +
                "(\n" +
                "    metric String, \n" +
                "    value Float64, \n" +
                "    timestamp UInt32, \n" +
                "    date Date, \n" +
                "    updated UInt32\n" +
                ") ENGINE = ReplacingMergeTree(date, (metric, timestamp), 8192, updated)\n"
        );
        jdbcTemplate.update(
            "CREATE TABLE default.metrics\n" +
                "(\n" +
                "    date Date DEFAULT toDate(0), \n" +
                "    name String, \n" +
                "    level UInt16, \n" +
                "    parent String, \n" +
                "    updated DateTime DEFAULT now(), \n" +
                "    status Enum8('SIMPLE' = 0, 'BAN' = 1, 'APPROVED' = 2, 'HIDDEN' = 3, 'AUTO_HIDDEN' = 4)\n" +
                ") ENGINE = ReplacingMergeTree(date, (parent, name), 1024, updated)\n"
        );
        jdbcTemplate.batchUpdate(
            "INSERT INTO default.metrics (name, level, parent) VALUES (?, ?, ?)",
            Arrays.asList(
                new Object[]{"dir1.", 1, "."},
                new Object[]{"dir1.metric1", 2, "dir1."},
                new Object[]{"dir1.metric2", 2, "dir1."},
                new Object[]{"dir1.metric3", 2, "dir1."},
                new Object[]{"dir2.", 1, "."},
                new Object[]{"dir2.metric1", 2, "dir2."},
                new Object[]{"dir2.metric2", 2, "dir2."},
                new Object[]{"dir2.metric3", 2, "dir2."}
            )
        );

        jdbcTemplate.update(
            "INSERT INTO default.data (metric, value, timestamp, date, updated) VALUES\n" +
                "    ('dir1.metric1', 0, 0, '1970-01-01', 42),\n" +
                "    ('dir1.metric1', 1, 1, '1970-01-01', 42),\n" +
                "    ('dir1.metric1', 2, 2, '1970-01-01', 42),\n" +
                "    ('dir1.metric1', 3, 3, '1970-01-01', 42),\n" +
                "    ('dir1.metric1', 4, 4, '1970-01-01', 42),\n" +
                "    ('dir1.metric1', 5, 5, '1970-01-01', 42),\n" +
                "    ('dir1.metric2', 0, 0, '1970-01-01', 42),\n" +
                "    ('dir1.metric2', 2, 1, '1970-01-01', 42),\n" +
                "    ('dir1.metric2', 4, 2, '1970-01-01', 42),\n" +
                "    ('dir1.metric2', 6, 3, '1970-01-01', 42),\n" +
                "    ('dir1.metric2', 8, 4, '1970-01-01', 42),\n" +
                "    ('dir1.metric2', 10, 5, '1970-01-01', 42),\n" +
                "    ('dir1.metric3', 0, 0, '1970-01-01', 42),\n" +
                "    ('dir1.metric3', 2, 1, '1970-01-01', 42),\n" +
                "    ('dir1.metric3', 4, 2, '1970-01-01', 42),\n" +
                "    ('dir1.metric3', 8, 3, '1970-01-01', 42),\n" +
                "    ('dir1.metric3', 16, 4, '1970-01-01', 42),\n" +
                "    ('dir1.metric3', 32, 5, '1970-01-01', 42);"
        );

        jdbcTemplate.update(
            "INSERT INTO default.data (metric, value, timestamp, date, updated) VALUES\n" +
                "    ('dir2.metric1', 1, 1, '1970-01-01', 42),\n" +
                "    ('dir2.metric1', 3, 3, '1970-01-01', 42),\n" +
                "    ('dir2.metric1', 5, 5, '1970-01-01', 42),\n" +
                "    ('dir2.metric2', 0, 0, '1970-01-01', 42),\n" +
                "    ('dir2.metric2', 4, 2, '1970-01-01', 42),\n" +
                "    ('dir2.metric2', 8, 4, '1970-01-01', 42),\n" +
                "    ('dir2.metric3', 2, 1, '1970-01-01', 42),\n" +
                "    ('dir2.metric3', 4, 2, '1970-01-01', 42),\n" +
                "    ('dir2.metric3', 8, 3, '1970-01-01', 42);"
        );
    }

    @Configuration
    @PropertySource(value = {"classpath:graphouse-default.properties", "classpath:test.properties"})
    @Import({MetricsConfig.class, ServerConfig.class})
    public static class Config {

        @Value("${graphouse.clickhouse.socket-timeout-seconds}")
        private int clickhouseSocketTimeoutSeconds;

        @Value("${graphouse.clickhouse.query-timeout-seconds}")
        private int clickhouseQueryTimeoutSeconds;


        @Bean
        public JdbcTemplate clickHouseJdbcTemplate() {
            String url = "jdbc:clickhouse://" + clickhouse.getContainerIpAddress() + ":"
                + clickhouse.getMappedPort(CLICKHOUSE_HTTP_PORT);

            ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
            clickHouseProperties.setSocketTimeout((int) TimeUnit.SECONDS.toMillis(clickhouseSocketTimeoutSeconds));

            ClickHouseDataSource dataSource = new ClickHouseDataSource(url, clickHouseProperties);
            JdbcTemplate jdbcTemplate = new JdbcTemplate();
            jdbcTemplate.setDataSource(dataSource);
            jdbcTemplate.setQueryTimeout(clickhouseQueryTimeoutSeconds);
            prepareTestData(jdbcTemplate);
            return jdbcTemplate;
        }

        @Bean
        public Monitoring monitoring() {
            return new Monitoring();
        }
    }
}
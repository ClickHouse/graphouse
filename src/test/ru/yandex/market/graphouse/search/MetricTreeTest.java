package ru.yandex.market.graphouse.search;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class MetricTreeTest {

    private MetricTree tree = new MetricTree();

    @Test
    public void testGlob() {
        Pattern pattern = MetricTree.createPattern("msh0[1-6]d_market_yandex_net");
        assertTrue(pattern.matcher("msh01d_market_yandex_net").matches());

        Pattern pattern2 = MetricTree.createPattern("one_min.market-front.timings-static.0_9[");


    }

    @Test
    public void testContainsExpression() throws Exception {
        assertTrue(MetricTree.containsExpressions("msh0[1-6]d_market_yandex_net"));
    }

    @Test
    public void testSearch() throws Exception {
        tree.add("five_sec.int_8742.x1");
        tree.add("five_sec.int_8742.x1");
        tree.add("five_sec.int_8743.x1");
        tree.add("five_sec.int_8742.x2");

        search("five_sec.int_874?.x1", "five_sec.int_8742.x1", "five_sec.int_8743.x1");
        search("five_sec.int_8742.x*", "five_sec.int_8742.x1", "five_sec.int_8742.x2");
        search("*", "five_sec.");
        search("five_sec.*", "five_sec.int_8742.", "five_sec.int_8743.");
    }


    private void assertNewOrUpdated(String metric, MetricStatus status) {
        long time = System.currentTimeMillis();
        MetricDescription metricDescription = tree.modify(metric, status);
        assert(metricDescription.getUpdateTimeMillis() >= time);
    }

    @Test
    public void testStatusesWorkflow() throws Exception {
        assertNewOrUpdated("five_sec.int_8742.x1", MetricStatus.SIMPLE);
        assertNewOrUpdated("five_sec.int_8742.x1", MetricStatus.SIMPLE);

        // BAN -> APPROVED
        tree.add("five_sec.int_8743.x1");
        assertNewOrUpdated("five_sec.int_8743.", MetricStatus.BAN);
        searchWithMessage("Dir is BANned, but we found it", "five_sec.*", "five_sec.int_8742.");
        searchWithMessage("Dir is BANned, but we found it's metric", "five_sec.int_8743.", "");
        assertNull("Dir is BANned, but we can add metric into it", tree.add("five_sec.int_8743.x0"));
        assertNull("Dir is BANned, but we can add dir into it", tree.add("five_sec.int_8743.new."));

        assertNewOrUpdated("five_sec.int_8743.", MetricStatus.APPROVED);
        search("five_sec.*", "five_sec.int_8742.", "five_sec.int_8743.");

        // HIDDEN
        search("five_sec.int_8742.*", "five_sec.int_8742.x1");
        assertNewOrUpdated("five_sec.int_8742.", MetricStatus.HIDDEN);
        searchWithMessage("Dir is HIDDEN, but we found it", "five_sec.*", "five_sec.int_8743.");
        searchWithMessage("Dir is HIDDEN, but we found it's metric", "five_sec.int_8742.*", "");
        assertNewOrUpdated("five_sec.int_8742.x2", MetricStatus.SIMPLE);
        search("five_sec.int_8742.*", "");
        assertNewOrUpdated("five_sec.int_8742.", MetricStatus.APPROVED);
        search("five_sec.*", "five_sec.int_8742.", "five_sec.int_8743.");

        // SIMPLE -> AUTO_HIDDEN -> SIMPLE
        search("five_sec.int_8742.*", "five_sec.int_8742.x1", "five_sec.int_8742.x2");
        assertNewOrUpdated("five_sec.int_8742.x2", MetricStatus.HIDDEN);
        searchWithMessage("Metric is HIDDEN, but we found it", "five_sec.int_8742.*", "five_sec.int_8742.x1");
        assertNewOrUpdated("five_sec.int_8742.x1", MetricStatus.HIDDEN);
        searchWithMessage("Dir is AUTO_HIDDEN, but we found it", "five_sec.*", "five_sec.int_8743.");
        assertNewOrUpdated("five_sec.int_8742.x3", MetricStatus.SIMPLE);
        searchWithMessage("We added new metric in AUTO_HIDDEN dir, but dir is still AUTO_HIDDEN",
                "five_sec.*", "five_sec.int_8742.", "five_sec.int_8743.");
        search("five_sec.int_8742.*", "five_sec.int_8742.x3");

        // AUTO_HIDDEN -> SIMPLE
        assertNewOrUpdated("five_sec.int_8742.", MetricStatus.AUTO_HIDDEN);
        searchWithMessage("Dir is AUTO_HIDDEN, but we found it", "five_sec.*", "five_sec.int_8743.");
        searchWithMessage("Dir is AUTO_HIDDEN, but we found it's metric", "five_sec.int_8742.*", "");

        tree.add("five_sec.int_8742.x2.y1");
        searchWithMessage("We added new metric, but dir is still AUTO_HIDDEN",
                "five_sec.*", "five_sec.int_8742.", "five_sec.int_8743.");
        search("five_sec.int_8742.*", "five_sec.int_8742.x2.", "five_sec.int_8742.x3");
    }

    private void search(String pattern, String... expected) throws IOException {
        searchWithMessage("", pattern, expected);
    }

    private void searchWithMessage(String message, String pattern, String... expected) throws IOException {
        Arrays.sort(expected);
        StringBuilder result = new StringBuilder();
        tree.search(pattern, result);
        String[] actual = result.toString().split("\\n");
        Arrays.sort(actual);
        assertArrayEquals(message + "\nFound " + result, expected, actual);
    }


}
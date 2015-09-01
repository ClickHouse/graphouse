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

    @Test
    public void testStatusesWorkflow() throws Exception {
        assertEquals(QueryStatus.NEW, tree.add("five_sec.int_8742.x1"));
        assertEquals(QueryStatus.UPDATED, tree.add("five_sec.int_8742.x1"));
        tree.add("five_sec.int_8742.x2");

        tree.add("five_sec.int_8743.x1");
        assertEquals(QueryStatus.UPDATED, tree.add("five_sec.int_8743.", MetricStatus.BAN));
        search("five_sec.*", "five_sec.int_8742.");
        search("five_sec.int_8743.", "");
        // assertEquals(QueryStatus.BAN, tree.add("five_sec.int_8743.x0"));
        search("five_sec.int_8743.", "");

        assertEquals(QueryStatus.UPDATED, tree.add("five_sec.int_8743.", MetricStatus.APPROVED));
        search("five_sec.*", "five_sec.int_8742.", "five_sec.int_8743.");

        // hide dir if all metrics already hidden five_sec.int_8742.[x1, x2]
        assertEquals(QueryStatus.UPDATED, tree.add("five_sec.int_8742.x2", MetricStatus.HIDDEN));
        search("five_sec.int_8742.*", "five_sec.int_8742.x1");
        assertEquals(QueryStatus.UPDATED, tree.add("five_sec.int_8742.x1", MetricStatus.HIDDEN));
        search("five_sec.*", "five_sec.int_8743.");

        // open dir if one metric is not hidden
        assertEquals(QueryStatus.NEW, tree.add("five_sec.int_8742.x3", MetricStatus.APPROVED));
        search("five_sec.*", "five_sec.int_8742.", "five_sec.int_8743.");
        search("five_sec.int_8742.*", "five_sec.int_8742.x3");
    }

    private void search(String pattern, String... expected) throws IOException {
        Arrays.sort(expected);
        StringBuilder result = new StringBuilder();
        tree.search(pattern, result);
        String[] actual = result.toString().split("\\n");
        Arrays.sort(actual);
        assertArrayEquals(expected, actual);
    }


}
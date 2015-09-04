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
        assertEquals(MetricStatus.SIMPLE, tree.add("five_sec.int_8742.x1").getStatus());
        assertEquals(MetricStatus.SIMPLE, tree.add("five_sec.int_8742.x1").getStatus());

        // BAN -> APPROVED
        tree.add("five_sec.int_8743.x1");
        assertEquals(MetricStatus.BAN, tree.modify("five_sec.int_8743.", MetricStatus.BAN).getStatus());
        searchWithMessage("Dir is BANned, but we found it", "five_sec.*", "five_sec.int_8742.");
        searchWithMessage("Dir is BANned, but we found it's metric", "five_sec.int_8743.", "");
        assertEquals("Dir is BANned, but we can add metric into it", null, tree.add("five_sec.int_8743.x0"));
        assertEquals("Dir is BANned, but we can add dir into it", null, tree.add("five_sec.int_8743.new."));

        assertEquals(MetricStatus.APPROVED, tree.modify("five_sec.int_8743.", MetricStatus.APPROVED).getStatus());
        search("five_sec.*", "five_sec.int_8742.", "five_sec.int_8743.");

        // HIDDEN
        search("five_sec.int_8742.*", "five_sec.int_8742.x1");
        assertEquals(MetricStatus.HIDDEN, tree.modify("five_sec.int_8742.", MetricStatus.HIDDEN).getStatus());
        searchWithMessage("Dir is HIDDEN, but we found it", "five_sec.*", "five_sec.int_8743.");
        searchWithMessage("Dir is HIDDEN, but we found it's metric", "five_sec.int_8742.*", "");
        assertEquals(MetricStatus.SIMPLE, tree.add("five_sec.int_8742.x2").getStatus());
        search("five_sec.int_8742.*", "five_sec.int_8742.x1", "five_sec.int_8742.x2");
        assertEquals(MetricStatus.APPROVED, tree.modify("five_sec.int_8742.", MetricStatus.APPROVED).getStatus());
        search("five_sec.*", "five_sec.int_8742.", "five_sec.int_8743.");

        // SIMPLE -> AUTO_HIDDEN -> SIMPLE
        search("five_sec.int_8742.*", "five_sec.int_8742.x1", "five_sec.int_8742.x2");
        assertEquals(MetricStatus.HIDDEN, tree.modify("five_sec.int_8742.x2", MetricStatus.HIDDEN).getStatus());
        searchWithMessage("Metric is HIDDEN, but we found it", "five_sec.int_8742.*", "five_sec.int_8742.x1");
        assertEquals(MetricStatus.HIDDEN, tree.modify("five_sec.int_8742.x1", MetricStatus.HIDDEN).getStatus());
        searchWithMessage("Dir is AUTO_HIDDEN, but we found it", "five_sec.*", "five_sec.int_8743.", "five_sec.int_8742."); //Cause "five_sec.int_8742." is Approved
        assertEquals(MetricStatus.SIMPLE, tree.add("five_sec.int_8742.x3").getStatus());
        searchWithMessage("We added new metric in AUTO_HIDDEN dir, but dir is still AUTO_HIDDEN",
            "five_sec.*", "five_sec.int_8742.", "five_sec.int_8743.");
        search("five_sec.int_8742.*", "five_sec.int_8742.x3");

        assertEquals(MetricStatus.SIMPLE, tree.add("five_sec.int_8742.x2.y1").getStatus());
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
package ru.yandex.market.graphouse.search;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class MetricTreeTest {

    private MetricTree tree = new MetricTree();

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

        tree.ban("five_sec.int_8743.");
        search("five_sec.*", "five_sec.int_8742.");
        search("five_sec.*", "five_sec.int_8742.");

    }


    private void search(String pattern, String... expected) {
        Arrays.sort(expected);
        String[] actual = tree.search(pattern).split("\\n");
        Arrays.sort(actual);
        assertArrayEquals(expected, actual);
    }
}
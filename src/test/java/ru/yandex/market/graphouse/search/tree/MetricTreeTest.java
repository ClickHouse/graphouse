package ru.yandex.market.graphouse.search.tree;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.Test;
import ru.yandex.market.graphouse.search.MetricStatus;
import ru.yandex.market.graphouse.search.tree.InMemoryMetricDir;
import ru.yandex.market.graphouse.search.tree.MetricTree;
import ru.yandex.market.graphouse.utils.AppendableWrapper;

import java.io.IOException;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MetricTreeTest {

    private MetricTree tree = new MetricTree(InMemoryMetricDir::new);

    public static Pattern createPattern(final String globPattern) {
        String result = globPattern.replace("*", "[-_0-9a-zA-Z]*");
        result = result.replace("?", "[-_0-9a-zA-Z]");
        try {
            return Pattern.compile(result);
        } catch (PatternSyntaxException e) {
            return null;
        }
    }

    @Test
    public void testGlob() {
        Multimap<String, String> pattern2Candidates = generate();
        for (Map.Entry<String, Collection<String>> pattern2CandidatesMap : pattern2Candidates.asMap().entrySet()) {
            String glob = pattern2CandidatesMap.getKey();
            Pattern pattern = createPattern(glob);
            if (pattern == null) {
                System.out.println("Wrong pattern " + glob);
                continue;
            }
            for (String node : pattern2CandidatesMap.getValue()) {
                System.out.println(String.format("%40s\t%40s\t%s", glob, node, pattern.matcher(node).matches()));
            }
        }
    }

    @Test
    public void testGlobPath() {
        PathMatcher matcher = MetricTree.createPathMatcher("asdf[");
        assertNull(matcher);

        Multimap<String, String> pattern2Candidates = generate();
        for (Map.Entry<String, Collection<String>> pattern2CandidatesMap : pattern2Candidates.asMap().entrySet()) {
            String glob = pattern2CandidatesMap.getKey();
            matcher = MetricTree.createPathMatcher(glob);
            if (matcher == null) {
                System.out.println("Wrong pattern " + glob);
                continue;
            }
            for (String node : pattern2CandidatesMap.getValue()) {
                System.out.println(String.format("%40s\t%40s\t%s", glob, node, MetricTree.matches(matcher, node)));
            }
        }
    }

    private Multimap<String, String> generate() {
        Multimap<String, String> pattern2Candidates = ArrayListMultimap.create();
        pattern2Candidates.putAll("msh0[1-6]d_market_yandex_net", Arrays.asList("msh01d_market_yandex_net", "msh03d_market_yandex_net"));
        pattern2Candidates.putAll("min.market-front*.e", Arrays.asList("min.market-front.e", "min.market-front-ugr.e"));
        pattern2Candidates.putAll("min.market-front{-ugr,-fol}.e", Arrays.asList("min.market-front-fol.e", "min.market-front-ugr.e"));
        pattern2Candidates.putAll("min.market-front{,-ugr,-fol}.e", Arrays.asList("min.market-front.e", "min.market-front-ugr.e"));
        return pattern2Candidates;
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
        Assert.assertEquals(MetricStatus.SIMPLE, tree.add("five_sec.int_8742.x1").getStatus());
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
        AppendableWrapper result = new AppendableWrapper();
        tree.search(pattern, result);
        String[] actual = result.toString().split("\\n");
        Arrays.sort(actual);
        assertArrayEquals(message + "\nFound " + result, expected, actual);
    }


}
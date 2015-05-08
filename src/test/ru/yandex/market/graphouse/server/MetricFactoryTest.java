package ru.yandex.market.graphouse.server;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 08/05/15
 */
public class MetricFactoryTest {

    private MetricFactory factory = new MetricFactory();

    @Test
    public void testProcessName() throws Exception {
        checkHostRedirect("one_min.zk03e_stat_yandex_net.sockstat_tcp.alloc", "one_min.HOST.zk03e_stat_yandex_net.sockstat_tcp.alloc");
        checkHostRedirect("one_min.zk03e_stat_yandex_ru.sockstat_tcp.alloc", "one_min.HOST.zk03e_stat_yandex_ru.sockstat_tcp.alloc");
        checkHostRedirect("one_min.some_service.safas.fasf", "one_min.some_service.safas.fasf");
    }

    private void checkHostRedirect(String metric, String expected) {
        assertEquals(expected, factory.processName(metric));
    }

    @Test
    public void testValidate() throws Exception {
        assertFalse(MetricFactory.validate("gdsgsgs"));
        assertFalse(MetricFactory.validate("one_min.fdsfdsfs..fdsfsfsd"));
        assertFalse(MetricFactory.validate("one_min.fdsfdsfs.fdsfsfsd."));
        assertFalse(MetricFactory.validate(".one_min.fdsfdsfs.fdsfsfsd"));
        assertFalse(MetricFactory.validate("one_min..x"));
        assertFalse(MetricFactory.validate("one_min.x.x.d.d.d.d.d.d.x.x.x.x.d"));
        assertFalse(MetricFactory.validate("ten_min.fdsfdsfs.fdsfsfsd"));
        assertTrue(MetricFactory.validate("one_min.fdsfdsfs.fdsfsfsd"));
    }
}
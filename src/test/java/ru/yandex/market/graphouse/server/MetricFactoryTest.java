package ru.yandex.market.graphouse.server;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 08/05/15
 */
public class MetricFactoryTest {

    private MetricFactory factory = new MetricFactory(null, null, true, "HOST", "yandex_net,yandex_ru");

    @Test
    public void testProcessName() throws Exception {
        checkHostRedirect("one_min.zk03e_stat_yandex_net.sockstat_tcp.alloc", "one_min.HOST.zk03e_stat_yandex_net.sockstat_tcp.alloc");
        checkHostRedirect("one_min.zk03e_stat_yandex_ru.sockstat_tcp.alloc", "one_min.HOST.zk03e_stat_yandex_ru.sockstat_tcp.alloc");
        checkHostRedirect("one_min.some_service.safas.fasf", "one_min.some_service.safas.fasf");
    }

    private void checkHostRedirect(String metric, String expected) {
        assertEquals(expected, factory.processName(metric));
    }

}
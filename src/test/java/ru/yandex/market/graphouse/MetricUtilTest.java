package ru.yandex.market.graphouse;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 03/02/2017
 */
public class MetricUtilTest {
    @Test
    public void getLastLevelName() throws Exception {
        Assert.assertEquals("b", MetricUtil.getLastLevelName("a.b.c"));
        Assert.assertEquals("c", MetricUtil.getLastLevelName("a.b.c."));
    }

}
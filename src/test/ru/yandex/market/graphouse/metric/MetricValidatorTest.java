package ru.yandex.market.graphouse.metric;


import ru.yandex.market.graphouse.MetricValidator;

import static org.junit.Assert.*;

public class MetricValidatorTest {

    @org.junit.Test
    public void testValidate() throws Exception {
        assertFalse(MetricValidator.validate("gdsgsgs"));
        assertFalse(MetricValidator.validate("one_min.fdsfdsfs..fdsfsfsd"));
        assertFalse(MetricValidator.validate("one_min.fdsfdsfs.fdsfsfsd."));
        assertFalse(MetricValidator.validate(".one_min.fdsfdsfs.fdsfsfsd"));
        assertFalse(MetricValidator.validate("one_min..x"));
        assertFalse(MetricValidator.validate("one_min.x.x.d.d.d.d.d.d.x.x.x.x.d"));
        assertFalse(MetricValidator.validate("ten_min.fdsfdsfs.fdsfsfsd"));

        assertTrue(MetricValidator.validate("one_min.fdsfdsfs.fdsfsfsd"));
    }


}
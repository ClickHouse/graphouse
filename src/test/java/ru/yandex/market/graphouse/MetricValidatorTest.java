package ru.yandex.market.graphouse;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 06/02/2017
 */
public class MetricValidatorTest {

    public static final MetricValidator VALIDATOR = new MetricValidator("[-_0-9a-zA-Z\\\\.]*$", 5, 500, 2, 20);

    @Test
    public void testValidate() throws Exception {
        valid("five_min.", true);
        invalid("gdsgsgs", true);

        valid("one_min.fdsfdsfs.fdsfsfsd", true);

        invalid("five_min.", false);
        invalid("-嘊-嘊嘍-嘍-aaa_.._tt_personal-billing-report_xml.0_995", true);
        invalid("market.mbo-front-iva.timings-dynamic.3febfdd52d4fea02xxx'x22<>_.._tt_personal-billing-report_xml_.0_995", true);
        invalid("market.mbo-front-iva.timings-dynamic.tt_personal-billing-report_xml_3febfdd52d4fea02xxx'x22<>_...0_99", true);

        invalid("one_min.fdsfdsfs..fdsfsfsd", true);
        invalid("one_min.fdsfdsfs.fdsfsfsd.", false);
        valid("one_min.fdsfdsfs.fdsfsfsd.", true);
        invalid(".one_min.fdsfdsfs.fdsfsfsd", true);
        invalid("one_min..x", true);
        invalid("one_min.x.x.d.d.d.d.d.d.x.x.x.x.d.x.d.d.x.d.d.d.d.d.d.x.x.x.x.d.x.d.d", true);
        invalid("nameadsfdsfdsfdsfsd", true);
        invalid("a.b", true);
        valid("dir1.metric1", true);
    }

    private void valid(String metric, boolean allowDirs) {
        Assert.assertTrue("Must be valid: " + metric, VALIDATOR.validate(metric, allowDirs));
    }

    private void invalid(String metric, boolean allowDirs) {
        Assert.assertFalse("Must be invalid: " + metric, VALIDATOR.validate(metric, allowDirs));
    }


}
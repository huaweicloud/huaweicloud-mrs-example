package com.huawei.fi.plugins.log;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.List;


public class DesensitiveXmlReaderTest {
    private static final String LOG_DESENSITIVE= "log4j2-desensitive.xml";

    @Before
    public void setUp()
            throws Exception {
    }

    @After
    public void tearDown() {

    }

    @Test
    public void readDesensitiveXmlTest() {
        DesensitiveXmlReader reader= new DesensitiveXmlReader();
        URL url= this.getClass().getResource("/" + LOG_DESENSITIVE);
        if (url != null) {
            List<Sensitive> list= reader.parse(url.getPath());
            Assert.assertTrue("The list size is expected to 4. ", list.size() == 4);
            for (Sensitive sensitive : list) {
                Assert.assertTrue(sensitive.getName().length() > 0);
                Assert.assertTrue(sensitive.getSize() > 1);
            }
        }

    }
}

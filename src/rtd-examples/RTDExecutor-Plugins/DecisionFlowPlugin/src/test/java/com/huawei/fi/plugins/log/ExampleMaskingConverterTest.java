package com.huawei.fi.plugins.log;


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class ExampleMaskingConverterTest {

    @Before
    public void setUp()
            throws Exception {
    }

    @After
    public void tearDown() {

    }

    @Test
    public void formatTest() {
        ExampleMaskingConverter converter= ExampleMaskingConverter.newInstance(new String[0]);

        SimpleMessage message= new SimpleMessage("{\"name\": \"Alice Wang\", \"idNo\": \"310110199999999999\", \"bankCardNo\": \"6220622062206220\", " +
                "\"mobile\": \"13598652364\"}");
        List<Property> list= new ArrayList<Property>();

        Log4jLogEvent event= new Log4jLogEvent("testLogger", null, "", Level.DEBUG, message, list, new Exception("test Exception"));

        StringBuilder output= new StringBuilder();
        converter.format(event, output);

        String result= "{\"name\": \"A*********\", \"idNo\": \"3*****************\", \"bankCardNo\": \"************6220\", \"mobile\": \"135****2364\"}";
        Assert.assertTrue(result.equals(output.toString()));
    }
}

package com.huawei.fi.rtd.drools.generator.exception;

import org.junit.Assert;
import org.junit.Test;

public class DroolsGeneratorExceptionTest {

    @Test
    public void create_exception_normal_and_return_success() {

        DroolsGeneratorException exceptionWithNone = new DroolsGeneratorException();
        DroolsGeneratorException exceptionWithMessage = new DroolsGeneratorException("test_01");
        DroolsGeneratorException exceptionWithThrowable = new DroolsGeneratorException(new Throwable("test_02"));
        DroolsGeneratorException exceptionWithMessageAndThrowable = new DroolsGeneratorException("test_03", new Throwable());

        Assert.assertTrue(exceptionWithNone != null);
        Assert.assertTrue(exceptionWithMessage.getMessage().equals("test_01"));

        Assert.assertTrue(exceptionWithThrowable.getCause().getMessage().equals("test_02"));

        Assert.assertTrue(exceptionWithMessageAndThrowable.getMessage().equals("test_03"));
        Assert.assertTrue(exceptionWithMessageAndThrowable.getCause().getMessage() == null);
    }

}

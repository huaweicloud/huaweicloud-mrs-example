package com.huawei.fi.rtd.drools.generator.exception;

public class DroolsGeneratorException extends RuntimeException {

    static final long serialVersionUID = -3958516993124229926L;

    public DroolsGeneratorException() {
        super();
    }

    public DroolsGeneratorException(String message) {
        super(message);
    }

    public DroolsGeneratorException(String message, Throwable cause) {
        super(message, cause);
    }

    public DroolsGeneratorException(Throwable cause) {
        super(cause);
    }
}

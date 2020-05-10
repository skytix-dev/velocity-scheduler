package com.skytix.velocity;

public class VelocityTaskException extends Exception {

    public VelocityTaskException() {
    }

    public VelocityTaskException(String message) {
        super(message);
    }

    public VelocityTaskException(String message, Throwable cause) {
        super(message, cause);
    }

    public VelocityTaskException(Throwable cause) {
        super(cause);
    }

    public VelocityTaskException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

package com.skytix.velocity.scheduler;

import com.skytix.velocity.VelocityTaskException;

public class TaskQueueFullException extends VelocityTaskException {

    public TaskQueueFullException() {
    }

    public TaskQueueFullException(String message) {
        super(message);
    }

    public TaskQueueFullException(String message, Throwable cause) {
        super(message, cause);
    }

    public TaskQueueFullException(Throwable cause) {
        super(cause);
    }

    public TaskQueueFullException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

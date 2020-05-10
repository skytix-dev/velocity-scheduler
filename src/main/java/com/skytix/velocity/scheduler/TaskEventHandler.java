package com.skytix.velocity.scheduler;

import org.apache.mesos.v1.scheduler.Protos;

public interface TaskEventHandler {
    public void onEvent(Protos.Event.Update aUpdateEvent);
}

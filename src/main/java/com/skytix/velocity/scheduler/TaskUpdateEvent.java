package com.skytix.velocity.scheduler;

import com.skytix.velocity.entities.VelocityTask;
import lombok.Builder;
import lombok.Getter;
import org.apache.mesos.v1.scheduler.Protos.Event.Update;

@Builder
@Getter
public class TaskUpdateEvent {
    private Update event;
    private VelocityTask task;
}

package com.skytix.velocity.entities;

import com.skytix.velocity.scheduler.Priority;
import com.skytix.velocity.scheduler.TaskRemote;
import org.apache.mesos.v1.Protos;

import java.time.LocalDateTime;

public interface Task extends Comparable<VelocityTask> {
    public Protos.TaskInfo getTaskInfo();
    public LocalDateTime getCreated();
    public Protos.TaskState getState();
    public Priority getPriority();
    public TaskRemote getRemote();
}

package com.skytix.velocity.scheduler;

import com.skytix.velocity.VelocityTaskException;
import com.skytix.velocity.entities.Task;
import com.skytix.velocity.entities.TaskDefinition;

import java.io.Closeable;

public interface MesosScheduler extends Closeable {
    Task launch(TaskDefinition aTaskDefinition) throws VelocityTaskException;
}

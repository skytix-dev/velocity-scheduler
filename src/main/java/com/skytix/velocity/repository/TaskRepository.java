package com.skytix.velocity.repository;

import com.skytix.velocity.VelocityTaskException;
import com.skytix.velocity.entities.Task;
import org.apache.mesos.v1.Protos;

import java.io.Closeable;
import java.util.List;

public interface TaskRepository<T extends Task> extends Closeable {
    public void queueTask(T aTask) throws VelocityTaskException;
    public void retryTask(T aTask) throws VelocityTaskException;
    public List<T> getActiveTasks();
    public T getTaskByTaskId(String aTaskId);
    public void updateTaskState(Protos.TaskID aTaskID, Protos.TaskState aTaskStatus);
    public List<Protos.TaskInfo.Builder> getMatchingWaitingTasks(Protos.Offer aOffer);
    public void launchTasks(List<Protos.TaskInfo> aTasks);
    public void completeTask(T aTask);
    public int getNumQueuedTasks();
    public int getNumActiveTasks();

}

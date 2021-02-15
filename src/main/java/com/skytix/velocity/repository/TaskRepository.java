package com.skytix.velocity.repository;

import com.skytix.velocity.VelocityTaskException;
import com.skytix.velocity.entities.Task;
import com.skytix.velocity.entities.VelocityTask;
import org.apache.mesos.v1.Protos;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

public interface TaskRepository<T extends Task> extends Closeable {
    public void queueTask(T aTask) throws VelocityTaskException;
    public void retryTask(T aTask) throws VelocityTaskException;
    public List<T> getQueuedTasks();
    public Set<T> getMissedTasks();
    public List<T> getActiveTasks();
    public T getTaskByTaskId(String aTaskId);
    public void updateTaskState(VelocityTask aTaskID, Protos.TaskState aTaskStatus);
    public List<VelocityTask> getMatchingWaitingTasks(Protos.Offer aOffer);
    public void launchTasks(List<VelocityTask> aTasks);
    public void completeTask(T aTask);
    public int getNumQueuedTasks();
    public int getNumActiveTasks();

}

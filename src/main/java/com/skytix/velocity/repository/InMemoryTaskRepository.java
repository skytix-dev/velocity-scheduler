package com.skytix.velocity.repository;

import com.skytix.velocity.TaskValidationException;
import com.skytix.velocity.VelocityTaskException;
import com.skytix.velocity.entities.TaskDefinition;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.mesos.MesosUtils;
import com.skytix.velocity.scheduler.OfferBucket;
import com.skytix.velocity.scheduler.OfferBucketFullException;
import com.skytix.velocity.scheduler.OfferPredicate;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.Protos;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class InMemoryTaskRepository implements TaskRepository<VelocityTask> {
    private final Map<String, VelocityTask> mTaskInfoByTaskId = new HashMap<>();
    private final Set<VelocityTask> mAwaitingTasks = new ConcurrentSkipListSet<>();
    private final Set<VelocityTask> mAwaitingGpuTasks = new ConcurrentSkipListSet<>();
    private final List<VelocityTask> mRunningTasks = new ArrayList<>();

    private final AtomicInteger mTotalWaitingTasks = new AtomicInteger(0);

    private double mWaitingCpu = 0;
    private double mWaitingMem = 0;
    private double mWaitingDisk = 0;
    private double mWaitingGpu = 0;

    private double mRunningCpu = 0;
    private double mRunningMem = 0;
    private double mRunningDisk = 0;
    private double mRunningGpu = 0;

    private int mTotalTaskCounter = 0;

    public InMemoryTaskRepository(MeterRegistry aMeterRegistry) {
        aMeterRegistry.gauge("velocity.guage.scheduler.numRunningTasks", mRunningTasks, List::size);
        aMeterRegistry.gauge("velocity.guage.scheduler.numWaitingTasks", mTotalWaitingTasks);
    }

    @Override
    public synchronized void queueTask(VelocityTask aTask) throws VelocityTaskException {
        final TaskDefinition definition = aTask.getTaskDefinition();

        if (definition.hadTaskId()) {
            final Protos.TaskInfo.Builder taskInfo = definition.getTaskInfo();
            final double taskGpus = MesosUtils.getGpus(taskInfo, 0);

            mTaskInfoByTaskId.put(taskInfo.getTaskId().getValue(), aTask);

            if (taskGpus > 0) {
                mAwaitingGpuTasks.add(aTask);

            } else {
                mAwaitingTasks.add(aTask);
            }

            incrementWaitingCounters(taskInfo);

        } else {
            throw new TaskValidationException("TaskInfo is missing a TaskID");
        }

    }

    @Override
    public synchronized void retryTask(VelocityTask aTask) throws VelocityTaskException {

        if (aTask != null) {
            decrementRunningCounters(aTask.getTaskInfo());

            if (aTask.getTaskRetries() < 3) {
                aTask.setStarted(false);
                aTask.setRemote(null);
                aTask.setTaskInfo(null);
                aTask.incrementRetry();

                queueTask(aTask);
            }

        }

    }

    @Override
    public void completeTask(VelocityTask aTask) {
        final Protos.TaskInfo taskInfo = aTask.getTaskInfo();
        final String taskId = taskInfo.getTaskId().getValue();

        mTotalTaskCounter++;

        if (mTaskInfoByTaskId.containsKey(taskId)) {
            mRunningTasks.remove(aTask);
            decrementRunningCounters(taskInfo);
            mTaskInfoByTaskId.remove(taskId);
        }
    }

    @Override
    public synchronized void launchTasks(List<Protos.TaskInfo> aTasks) {

        for (Protos.TaskInfo task : aTasks) {
            final VelocityTask velocityTask = mTaskInfoByTaskId.get(task.getTaskId().getValue());
            final double taskGpus = MesosUtils.getGpus(task, 0);

            velocityTask.setTaskInfo(task);

            if (taskGpus > 0) {
                mAwaitingGpuTasks.remove(velocityTask);

            } else {
                mAwaitingTasks.remove(velocityTask);
            }

            decrementWaitingCounters(task);

            mRunningTasks.add(velocityTask);
            incrementRunningCounters(task);
        }

    }

    @Override
    public synchronized void updateTaskState(Protos.TaskID aTaskID, Protos.TaskState aTaskState) {

        if (mTaskInfoByTaskId.containsKey(aTaskID.getValue())) {
            final VelocityTask velocityTask = mTaskInfoByTaskId.get(aTaskID.getValue());
            velocityTask.setState(aTaskState);

            if (aTaskState.equals(Protos.TaskState.TASK_STARTING)) {
                velocityTask.setStarted(true);
            }

        }

    }


    @Override
    public synchronized List<Protos.TaskInfo.Builder> getMatchingWaitingTasks(Protos.Offer aOffer) {
        final OfferBucket bucket = new OfferBucket(aOffer);

        try {
            // If the offer contains any GPU resources, we will try and launch as many as we can first before
            // scheduling any other tasks.
            if (MesosUtils.getGpus(aOffer, 0) > 0) {
                populateOfferBucket(aOffer, bucket, mAwaitingGpuTasks);
            }

            populateOfferBucket(aOffer, bucket, mAwaitingTasks);

        } catch (OfferBucketFullException aE) {
            /* Bucket is full so stop whatever we are doing. */
        }

        return bucket.getAllocatedTasks();
    }

    public VelocityTask getTaskByTaskId(String aTaskId) {
        return mTaskInfoByTaskId.getOrDefault(aTaskId, null);
    }

    public synchronized int getNumQueuedTasks() {
        return mTotalWaitingTasks.get();
    }

    public synchronized int getNumActiveTasks() {
        return mRunningTasks.size();
    }

    @Override
    public void close() throws IOException {

    }

    private void populateOfferBucket(Protos.Offer aOffer, OfferBucket aOfferBucket, Set<VelocityTask> aAwaitingTasks) throws OfferBucketFullException {

        for (VelocityTask velocityTask : aAwaitingTasks) {
            final TaskDefinition taskDefinition = velocityTask.getTaskDefinition();
            final Protos.TaskInfo.Builder taskInfo = taskDefinition.getTaskInfo();

            if (aOfferBucket.hasResources(taskInfo)) {

                if (taskDefinition.hasConditions()) {
                    boolean condition = true;

                    for (OfferPredicate predicate : taskDefinition.getConditions()) {
                        condition = condition && predicate.test(aOffer);
                    }

                    if (condition) {
                        aOfferBucket.add(taskInfo);

                    }

                } else {
                    aOfferBucket.add(taskInfo);
                }

            }

        }

    }

    private void incrementWaitingCounters(Protos.TaskInfoOrBuilder aTaskInfo) {
        mTotalWaitingTasks.incrementAndGet();

        mWaitingCpu += MesosUtils.getNamedResourceScalar("cpus", aTaskInfo, 0);
        mWaitingMem += MesosUtils.getNamedResourceScalar("mem", aTaskInfo, 0);
        mWaitingDisk += MesosUtils.getNamedResourceScalar("disk", aTaskInfo, 0);
        mWaitingGpu += MesosUtils.getNamedResourceScalar("gpus", aTaskInfo, 0);
    }

    private void decrementWaitingCounters(Protos.TaskInfoOrBuilder aTaskInfo) {
        mTotalWaitingTasks.decrementAndGet();

        mWaitingCpu -= MesosUtils.getNamedResourceScalar("cpus", aTaskInfo, 0);
        mWaitingMem -= MesosUtils.getNamedResourceScalar("mem", aTaskInfo, 0);
        mWaitingDisk -= MesosUtils.getNamedResourceScalar("disk", aTaskInfo, 0);
        mWaitingGpu -= MesosUtils.getNamedResourceScalar("gpus", aTaskInfo, 0);
    }

    private void incrementRunningCounters(Protos.TaskInfoOrBuilder aTaskInfo) {
        mRunningCpu += MesosUtils.getNamedResourceScalar("cpus", aTaskInfo, 0);
        mRunningMem += MesosUtils.getNamedResourceScalar("mem", aTaskInfo, 0);
        mRunningDisk += MesosUtils.getNamedResourceScalar("disk", aTaskInfo, 0);
        mRunningGpu += MesosUtils.getNamedResourceScalar("gpus", aTaskInfo, 0);
    }

    private void decrementRunningCounters(Protos.TaskInfoOrBuilder aTaskInfo) {
        mRunningCpu -= MesosUtils.getNamedResourceScalar("cpus", aTaskInfo, 0);
        mRunningMem -= MesosUtils.getNamedResourceScalar("mem", aTaskInfo, 0);
        mRunningDisk -= MesosUtils.getNamedResourceScalar("disk", aTaskInfo, 0);
        mRunningGpu -= MesosUtils.getNamedResourceScalar("gpus", aTaskInfo, 0);
    }

}

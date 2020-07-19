package com.skytix.velocity.repository;

import com.google.common.util.concurrent.AtomicDouble;
import com.skytix.schedulerclient.mesos.MesosConstants;
import com.skytix.velocity.TaskValidationException;
import com.skytix.velocity.VelocityTaskException;
import com.skytix.velocity.entities.TaskDefinition;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.mesos.MesosUtils;
import com.skytix.velocity.scheduler.*;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.Protos;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class InMemoryTaskRepository implements TaskRepository<VelocityTask> {
    private final VelocitySchedulerConfig mConfig;
    private final Map<String, VelocityTask> mTaskInfoByTaskId = new HashMap<>();
    private final Semaphore mTaskQueue;
    private final Map<Enum<? extends Priority>, Set<VelocityTask>> mAwaitingTasks = new HashMap<>();
    private final Map<Enum<? extends Priority>, Set<VelocityTask>> mAwaitingGpuTasks = new HashMap<>();
    private final List<VelocityTask> mRunningTasks = new ArrayList<>();

    private final AtomicInteger mTotalWaitingTasks = new AtomicInteger(0);

    private AtomicDouble mWaitingCpu = new AtomicDouble(0);
    private AtomicDouble mWaitingMem = new AtomicDouble(0);
    private AtomicDouble mWaitingDisk = new AtomicDouble(0);
    private AtomicDouble mWaitingGpu = new AtomicDouble(0);

    private AtomicDouble mRunningCpu = new AtomicDouble(0);
    private AtomicDouble mRunningMem = new AtomicDouble(0);
    private AtomicDouble mRunningDisk = new AtomicDouble(0);
    private AtomicDouble mRunningGpu = new AtomicDouble(0);

    private final AtomicInteger mTotalTaskCounter = new AtomicInteger(0);
    private final List<Enum<? extends Priority>> mTaskPriorities;

    public InMemoryTaskRepository(MeterRegistry aMeterRegistry, VelocitySchedulerConfig aConfig) {
        mConfig = aConfig;

        if (mConfig.getPriorites() != null) {
            mTaskPriorities = Arrays.asList(mConfig.getPriorites().getEnumConstants());

        } else {
            mTaskPriorities = Arrays.asList(DefaultPriority.values());
        }

        final Integer maxTaskQueueSize = aConfig.getMaxTaskQueueSize();

        if (maxTaskQueueSize > 0) {
            mTaskQueue = new Semaphore(maxTaskQueueSize);

        } else {
            throw new IllegalArgumentException("maxTaskQueueSize must be greater than zero");
        }

        configurePriorityQueues();
        configureMetrics(aMeterRegistry);
    }

    @Override
    public synchronized List<VelocityTask> getActiveTasks() {
        return mRunningTasks;
    }

    @Override
    public synchronized List<VelocityTask> getQueuedTasks() {
        final List<VelocityTask> tasks = new ArrayList<>();

        mAwaitingGpuTasks.forEach((key, value) -> tasks.addAll(value));
        mAwaitingTasks.forEach((key, value) -> tasks.addAll(value));

        return tasks;
    }

    @Override
    public synchronized void queueTask(VelocityTask aTask) throws VelocityTaskException {
        queueTask(aTask, false);
    }

    private synchronized void queueTask(VelocityTask aTask, boolean aIsRetry) throws VelocityTaskException {
        final TaskDefinition definition = aTask.getTaskDefinition();
        final Enum<? extends Priority> priority = definition.getPriority() != null ? definition.getPriority() : DefaultPriority.STANDARD;

        if (definition.hasTaskId()) {

            try {

                if (aIsRetry || mTaskQueue.tryAcquire(mConfig.getTaskQueueFullWaitTimeout(), mConfig.getTaskQueueFullWaitTimeoutUnit())) {
                    final Protos.TaskInfo.Builder taskInfo = definition.getTaskInfo();
                    final double taskGpus = MesosUtils.getGpus(taskInfo, 0);

                    mTaskInfoByTaskId.put(taskInfo.getTaskId().getValue(), aTask);

                    if (taskGpus > 0) {

                        if (mConfig.isEnableGPUResources()) {
                            mAwaitingGpuTasks.get(priority).add(aTask);

                        } else {
                            throw new TaskValidationException("Unable to request GPU as GPU resources have not been enabled in the scheduler config");
                        }

                    } else {
                        mAwaitingTasks.get(priority).add(aTask);
                    }

                    incrementWaitingCounters(taskInfo);

                } else {
                    throw new TaskQueueFullException();
                }

            } catch (InterruptedException aE) {
                throw new VelocityTaskException(aE);
            }

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

                queueTask(aTask, true);
            }

        }

    }

    @Override
    public void completeTask(VelocityTask aTask) {
        final Protos.TaskInfo taskInfo = aTask.getTaskInfo();
        final String taskId = taskInfo.getTaskId().getValue();

        mTotalTaskCounter.incrementAndGet();

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
            final Enum<? extends Priority> priority = velocityTask.getTaskDefinition().getPriority();

            velocityTask.setTaskInfo(task);

            if (taskGpus > 0) {
                mAwaitingGpuTasks.get(priority).remove(velocityTask);

            } else {
                mAwaitingTasks.get(priority).remove(velocityTask);
            }

            decrementWaitingCounters(task);

            mRunningTasks.add(velocityTask);
            incrementRunningCounters(task);
            mTaskQueue.release();
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

        // If the offer contains any GPU resources, we will try and launch as many as we can first before
        // scheduling any other tasks.

        if (mConfig.isEnableGPUResources()) {

            if (MesosUtils.getGpus(aOffer, 0) > 0) {
                populateOfferBucket(aOffer, bucket, mAwaitingGpuTasks);

                // For single-node clusters or where you want to allow non-gpu workloads on gpu-enabled agents, schedule any more awaiting tasks.
                if (!mConfig.isRestrictedGpuScheduling()) {
                    populateOfferBucket(aOffer, bucket, mAwaitingTasks);
                }

            } else {
                populateOfferBucket(aOffer, bucket, mAwaitingTasks);
            }

        } else {
            populateOfferBucket(aOffer, bucket, mAwaitingTasks);
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

    private void populateOfferBucket(Protos.Offer aOffer, OfferBucket aOfferBucket, Map<Enum<? extends Priority>, Set<VelocityTask>> aAwaitingTasks) {

        mTaskPriorities.stream().sorted(Comparator.comparing(Enum::ordinal)).forEach((priority) -> {

            for (VelocityTask velocityTask : aAwaitingTasks.get(priority)) {
                final TaskDefinition taskDefinition = velocityTask.getTaskDefinition();
                final Protos.TaskInfo.Builder taskInfo = taskDefinition.getTaskInfo();
                final double memoryTolerance = taskDefinition.getMemoryTolerance();

                try {
                    final boolean meetsConditions;

                    if (taskDefinition.hasConditions()) {
                        boolean condition = true;

                        for (OfferPredicate predicate : taskDefinition.getConditions()) {
                            condition = condition && predicate.test(aOffer);
                        }

                        meetsConditions = condition;

                    } else {
                        meetsConditions = true;
                    }

                    if (meetsConditions) {

                        if (aOfferBucket.hasResources(taskInfo)) {
                            aOfferBucket.add(taskInfo);

                        } else {

                            if (aOfferBucket.hasCpuResources(taskInfo) && aOfferBucket.hasDiskResources(taskInfo) && aOfferBucket.hasGpuResources(taskInfo)) {
                                // If we have enough of all other resources, check the memory tolerance.
                                if (memoryTolerance > 0) {
                                    final double memDemanded = MesosUtils.getMem(taskInfo, 0);
                                    final double minMemoryDemanded = memDemanded - (memDemanded * (memoryTolerance / 100));
                                    final double availableMemory = aOfferBucket.getOfferMem() - aOfferBucket.getAllocatedMem();

                                    if (availableMemory >= minMemoryDemanded) {

                                        log.info(String.format(
                                                "Task '%s' demanded %fM of memory with a minimum threshold of %fM. Using remaining %fM available memory on the offer.",
                                                taskInfo.getTaskId().getValue(),
                                                memDemanded,
                                                minMemoryDemanded,
                                                availableMemory
                                        ));

                                        taskInfo.getResourcesList().remove(MesosUtils.getNamedResource(MesosConstants.SCALAR_MEM, taskInfo));
                                        taskInfo.addResources(MesosUtils.createMemResource(availableMemory));

                                        aOfferBucket.add(taskInfo);
                                    }

                                }

                            }

                        }

                    }

                } catch (OfferBucketFullException aE) {
                    break;
                }

            }

        });

    }

    private void incrementWaitingCounters(Protos.TaskInfoOrBuilder aTaskInfo) {
        mTotalWaitingTasks.incrementAndGet();

        mWaitingCpu.addAndGet(MesosUtils.getNamedResourceScalar("cpus", aTaskInfo, 0));
        mWaitingMem.addAndGet(MesosUtils.getNamedResourceScalar("mem", aTaskInfo, 0));
        mWaitingDisk.addAndGet(MesosUtils.getNamedResourceScalar("disk", aTaskInfo, 0));
        mWaitingGpu.addAndGet(MesosUtils.getNamedResourceScalar("gpus", aTaskInfo, 0));
    }

    private void decrementWaitingCounters(Protos.TaskInfoOrBuilder aTaskInfo) {
        mTotalWaitingTasks.decrementAndGet();

        mWaitingCpu.set(mWaitingCpu.get() - MesosUtils.getNamedResourceScalar("cpus", aTaskInfo, 0));
        mWaitingMem.set(mWaitingMem.get() - MesosUtils.getNamedResourceScalar("mem", aTaskInfo, 0));
        mWaitingDisk.set(mWaitingDisk.get() - MesosUtils.getNamedResourceScalar("disk", aTaskInfo, 0));
        mWaitingGpu.set(mWaitingGpu.get() - MesosUtils.getNamedResourceScalar("gpus", aTaskInfo, 0));
    }

    private void incrementRunningCounters(Protos.TaskInfoOrBuilder aTaskInfo) {
        mRunningCpu.addAndGet(MesosUtils.getNamedResourceScalar("cpus", aTaskInfo, 0));
        mRunningMem.addAndGet(MesosUtils.getNamedResourceScalar("mem", aTaskInfo, 0));
        mRunningDisk.addAndGet(MesosUtils.getNamedResourceScalar("disk", aTaskInfo, 0));
        mRunningGpu.addAndGet(MesosUtils.getNamedResourceScalar("gpus", aTaskInfo, 0));
    }

    private void decrementRunningCounters(Protos.TaskInfoOrBuilder aTaskInfo) {
        mRunningCpu.set(mRunningCpu.get() - MesosUtils.getNamedResourceScalar("cpus", aTaskInfo, 0));
        mRunningMem.set(mRunningMem.get() - MesosUtils.getNamedResourceScalar("mem", aTaskInfo, 0));
        mRunningDisk.set(mRunningDisk.get() - MesosUtils.getNamedResourceScalar("disk", aTaskInfo, 0));
        mRunningGpu.set(mRunningGpu.get() - MesosUtils.getNamedResourceScalar("gpus", aTaskInfo, 0));
    }

    private void configurePriorityQueues() {
        mTaskPriorities.forEach((priority) -> mAwaitingTasks.put(priority, new ConcurrentSkipListSet<>()));
        mTaskPriorities.forEach((priority) -> mAwaitingGpuTasks.put(priority, new ConcurrentSkipListSet<>()));
    }

    private void configureMetrics(MeterRegistry aMeterRegistry) {
        aMeterRegistry.gauge("velocity.gauge.scheduler.numRunningTasks", mRunningTasks, List::size);
        aMeterRegistry.gauge("velocity.gauge.scheduler.totalWaitingTasks", mTotalWaitingTasks, AtomicInteger::get);

        mTaskPriorities.forEach((priority) -> {
            aMeterRegistry.gauge(String.format("velocity.gauge.scheduler.numWaitingTasks_%s", priority.name()), mAwaitingTasks.get(priority), Set::size);
            aMeterRegistry.gauge(String.format("velocity.gauge.scheduler.numWaitingGpuTasks_%s", priority.name()), mAwaitingGpuTasks.get(priority), Set::size);
        });

        aMeterRegistry.gauge("velocity.gauge.scheduler.numTotalTasks", mTotalTaskCounter, AtomicInteger::get);
        aMeterRegistry.gauge("velocity.gauge.scheduler.numWaitingCpu", mWaitingCpu, AtomicDouble::get);
        aMeterRegistry.gauge("velocity.gauge.scheduler.numWaitingMem", mWaitingMem, AtomicDouble::get);
        aMeterRegistry.gauge("velocity.gauge.scheduler.numWaitingDisk", mWaitingDisk, AtomicDouble::get);
        aMeterRegistry.gauge("velocity.gauge.scheduler.numWaitingGpu", mWaitingGpu, AtomicDouble::get);
        aMeterRegistry.gauge("velocity.gauge.scheduler.numRunningCpu", mRunningCpu, AtomicDouble::get);
        aMeterRegistry.gauge("velocity.gauge.scheduler.numRunningMem", mRunningMem, AtomicDouble::get);
        aMeterRegistry.gauge("velocity.gauge.scheduler.numRunningDisk", mRunningDisk, AtomicDouble::get);
        aMeterRegistry.gauge("velocity.gauge.scheduler.numRunningGpu", mRunningGpu, AtomicDouble::get);
    }

}

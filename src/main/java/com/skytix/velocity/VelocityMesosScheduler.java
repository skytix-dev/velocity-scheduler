package com.skytix.velocity;

import com.skytix.schedulerclient.Scheduler;
import com.skytix.velocity.entities.TaskDefinition;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.mesos.MesosUtils;
import com.skytix.velocity.repository.InMemoryTaskRepository;
import com.skytix.velocity.repository.TaskRepository;
import com.skytix.velocity.scheduler.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.scheduler.Protos;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class VelocityMesosScheduler implements MesosScheduler {
    private Scheduler mMesosScheduler;
    private VelocitySchedulerHandler mSchedulerHandler;
    private final SubmissionPublisher<VelocityTask> mNewTaskPublisher;
    private boolean mRunning = true;

    private final TaskRepository<VelocityTask> mTaskRepository;
    private final MeterRegistry mMeterRegistry;
    private final VelocitySchedulerConfig mSchedulerConfig;
    private final Object mErrorMonitor = new Object();
    private final AtomicReference<RunningState> mSchedulerRunningState = new AtomicReference<>(RunningState.STOPPED);

    private final ExecutorService mMainThreadPool = Executors.newFixedThreadPool(6);
    private final ScheduledExecutorService mTaskGeneralThreadPool = Executors.newScheduledThreadPool(5);

    private final Counter mTaskLaunchCounter;

    private ScheduledFuture<?> mHeartbeatTask;
    private ScheduledFuture<?> mReconcileTask;
    private final Future<?> mReconnectTask;

    public VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig) {
        this(aSchedulerConfig, new SimpleMeterRegistry());
    }

    public VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig, MeterRegistry aMeterRegistry) {
        this(aSchedulerConfig, aMeterRegistry, new InMemoryTaskRepository(aMeterRegistry, aSchedulerConfig));
    }

    private VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig, MeterRegistry aMeterRegistry, TaskRepository<VelocityTask> aTaskRepository) {
        mSchedulerConfig = aSchedulerConfig;
        mMeterRegistry = aMeterRegistry;
        mTaskRepository = aTaskRepository;

        mNewTaskPublisher = new SubmissionPublisher<>(mMainThreadPool, 1000);
        mNewTaskPublisher.subscribe(new TaskSubscriber(mTaskRepository));

        mReconnectTask = mTaskGeneralThreadPool.submit(() -> {

            try {

                while (mRunning) {

                    synchronized (mErrorMonitor) {
                        mErrorMonitor.wait();

                        if (mSchedulerRunningState.get().equals(RunningState.STOPPED)) {
                            handleReconnect();
                        }

                    }

                }

            } catch (InterruptedException aE) {
                // Do nothing.  We will exit.
            }

        });

        mTaskLaunchCounter = mMeterRegistry.counter("velocity.counter.scheduler.taskLaunch");

        handleReconnect();

    }

    private synchronized void handleReconnect() {

        try {

            if (mSchedulerRunningState.get().equals(RunningState.STOPPED)) {
                final VelocitySchedulerHandler schedulerHandler = createSchedulerHandler();

                mSchedulerRunningState.set(RunningState.STARTING);

                do {

                    try {

                        mMesosScheduler = Scheduler.newScheduler(
                                mSchedulerConfig,
                                schedulerHandler
                        );

                        mSchedulerHandler = schedulerHandler;
                        mSchedulerRunningState.set(RunningState.RUNNING);

                        return;

                    } catch (IOException aE) {
                        log.error(aE.getMessage(), aE);
                    }

                    log.error("Unable to connect to master.  Sleeping for 2 seconds before retrying...");
                    Thread.sleep(2000);

                } while (mSchedulerRunningState.get().equals(RunningState.STARTING));

            }

        } catch (InterruptedException aE) {
            // Do nothing.  Just exit.
        }

    }

    private VelocitySchedulerHandler createSchedulerHandler() {

        return new VelocitySchedulerHandler(
                mTaskRepository,
                mMeterRegistry,
                mSchedulerConfig,
                mMainThreadPool,
                mTaskGeneralThreadPool,
                mNewTaskPublisher
        ) {

            @Override
            public void onSubscribe(Protos.Event.Subscribed aSubscribeEvent) {
                super.onSubscribe(aSubscribeEvent);

                final AtomicInteger failures = new AtomicInteger(0);

                mHeartbeatTask = mTaskGeneralThreadPool.scheduleAtFixedRate(
                        () -> {

                            if (mSchedulerRunningState.get().equals(RunningState.RUNNING)) {
                                final Duration duration = Duration.between(this.getLastHeartbeat() != null ? this.getLastHeartbeat() : LocalDateTime.now(), LocalDateTime.now());
                                final int heartbeatInterval = this.getHeartbeatInterval();

                                if (duration.getSeconds() > heartbeatInterval + mSchedulerConfig.getHeartbeatDelaySeconds()) {
                                    failures.incrementAndGet();

                                } else {
                                    failures.set(0);
                                }

                                if (failures.get() > 1) {
                                    log.error("Missed 2 heartbeat intervals.  Triggering reconnection to masters");
                                    this.onHeartbeatFail();
                                    return;
                                }

                            }

                        },
                        0,
                        getHeartbeatInterval(),
                        TimeUnit.SECONDS
                );

                // Once every 15 minutes is recommended
                mReconcileTask = mTaskGeneralThreadPool.scheduleAtFixedRate(
                        () -> {
                            mMesosScheduler.getRemote().reconcile(MesosUtils.buildReconcileTasks(mTaskRepository.getActiveTasks()));
                        },
                        0,
                        15,
                        TimeUnit.MINUTES
                );

            }

            @Override
            public void onTerminate(Exception aException) {
                super.onTerminate(aException);
                log.error(String.format("Scheduler terminated: %s. Reconnecting", aException.getMessage()), aException);

                if (mSchedulerRunningState.get().equals(RunningState.RUNNING)) {
                    notifyErrorMonitor();
                }

            }

            @Override
            public void onDisconnect() {
                super.onDisconnect();

                mReconcileTask.cancel(true);
                mHeartbeatTask.cancel(true);

                log.error("Scheduler disconnected from the master. Reconnecting");

                if (mSchedulerRunningState.get().equals(RunningState.RUNNING)) {
                    notifyErrorMonitor();
                }

            }

            @Override
            public void onHeartbeatFail() {

                try {
                    mMesosScheduler.close();
                    onDisconnect();

                } catch (IOException aE) {
                    log.error(aE.getMessage(), aE);
                }

            }

            private void notifyErrorMonitor() {

                synchronized (mErrorMonitor) {
                    mSchedulerRunningState.set(RunningState.STOPPED);
                    mErrorMonitor.notify();
                }

            }

        };

    }

    @Override
    public VelocityTask launch(TaskDefinition aTaskDefinition) throws VelocityTaskException {
        validateTaskDefinition(aTaskDefinition);

        mTaskLaunchCounter.increment();

        final VelocityTask task = VelocityTask.builder()
                .taskDefinition(aTaskDefinition)
                .created(LocalDateTime.now())
                .build();

        if (mTaskRepository.getNumQueuedTasks() == 0) {
            mMesosScheduler.getRemote().revive(Collections.emptyList());
        }

        mNewTaskPublisher.submit(task);

        return task;
    }

    public int getNumQueuedTasks() {
        return mTaskRepository.getNumQueuedTasks();
    }

    public int getNumActiveTasks() {
        return mTaskRepository.getNumActiveTasks();
    }

    public Map<String, VelocityTask> getActiveTasksById() {
        final List<VelocityTask> activeTasks = mTaskRepository.getActiveTasks();

        if (!activeTasks.isEmpty()) {
            return activeTasks.stream().collect(Collectors.toMap(task -> task.getTaskInfo().getTaskId().getValue(), task -> task));

        } else {
            return Collections.emptyMap();
        }

    }

    public Map<String, VelocityTask> getQueuedTasksById() {
        final List<VelocityTask> queuedTasks = mTaskRepository.getQueuedTasks();

        if (!queuedTasks.isEmpty()) {
            return queuedTasks.stream().collect(Collectors.toMap(task -> task.getTaskDefinition().getTaskId(), task -> task));

        } else {
            return Collections.emptyMap();
        }

    }

    public VelocityTask getTaskById(String aTaskId) {
        return mTaskRepository.getTaskByTaskId(aTaskId);
    }

    public LocalDateTime getLastHeartbeat() {

        if (mSchedulerHandler != null) {
            return mSchedulerHandler.getLastHeartbeat();

        } else {
            return null;
        }

    }

    /**
     * Wait till all tasks have been completed and then send a teardown call to the Master.
     */
    public void drainAndTeardown() throws Exception {
        waitTillEmpty();

        log.info(String.format("Scheduler is empty.  Tearing down framework: %s", mSchedulerConfig.getFrameworkID()));

        stop();
        mMesosScheduler.getRemote().teardown();
    }

    private void validateTaskDefinition(TaskDefinition aTaskDefinition) throws TaskValidationException {

        if (!aTaskDefinition.hasTaskId()) {
            throw new TaskValidationException("Task definition must have a Task ID");
        }

        // Will have more other validations to perform in due course as they are discovered.
    }

    private void stop() {
        mSchedulerRunningState.set(RunningState.STOPPED);
        mRunning = false;
        mReconnectTask.cancel(true);
        mNewTaskPublisher.close();
        mTaskGeneralThreadPool.shutdown();
        mMainThreadPool.shutdown();
    }

    public void drainAndClose() throws Exception {
        waitTillEmpty();
        log.info("Scheduler is empty.  Now closing.");
        close();
    }

    private void waitTillEmpty() throws InterruptedException {
        int numActiveTasks = getNumActiveTasks();
        int numQueuedTasks = getNumQueuedTasks();
        int i = 0;

        while (numActiveTasks > 0 || numQueuedTasks > 0) {
            log.info(String.format("Waiting on task completion.  #Queued: %d, #Active: %d.", numQueuedTasks, numActiveTasks));
            i++;

            if (i % 20 == 0) {
                log.info(String.format("Performing reconciliation on %d remaining tasks", numActiveTasks));
                mMesosScheduler.getRemote().reconcile(MesosUtils.buildReconcileTasks(mTaskRepository.getActiveTasks())); // If we missed their event message, this will cause it to be resent.
                i = 1;

            } else {
                i++;
            }

            Thread.sleep(2000);

            numActiveTasks = getNumActiveTasks();
            numQueuedTasks = getNumQueuedTasks();
        }

    }

    @Override
    public void close() throws IOException {
        stop();
        mMesosScheduler.getRemote().exit();
    }

}

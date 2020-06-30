package com.skytix.velocity;

import com.skytix.schedulerclient.Scheduler;
import com.skytix.velocity.entities.TaskDefinition;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.repository.InMemoryTaskRepository;
import com.skytix.velocity.repository.TaskRepository;
import com.skytix.velocity.scheduler.MesosScheduler;
import com.skytix.velocity.scheduler.RunningState;
import com.skytix.velocity.scheduler.VelocitySchedulerConfig;
import com.skytix.velocity.scheduler.VelocitySchedulerHandler;
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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class VelocityMesosScheduler implements MesosScheduler {
    private Scheduler mMesosScheduler;
    private VelocitySchedulerHandler mSchedulerHandler;
    private boolean mRunning = true;

    private final TaskRepository<VelocityTask> mTaskRepository;
    private final MeterRegistry mMeterRegistry;
    private final VelocitySchedulerConfig mSchedulerConfig;
    private final ForkJoinTask<?> mReconnectTask;
    private final Object mErrorMonitor = new Object();
    private final AtomicReference<RunningState> mSchedulerRunningState = new AtomicReference<>(RunningState.STOPPED);

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

        mReconnectTask = ForkJoinPool.commonPool().submit(() -> {

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
                log.error(aE.getMessage(), aE);
            }

        });

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
            log.error("Interrupted");
        }

    }

    private VelocitySchedulerHandler createSchedulerHandler() {

        return new VelocitySchedulerHandler(
                mTaskRepository,
                mMeterRegistry,
                mSchedulerConfig
        ) {

            @Override
            public void onSubscribe(Protos.Event.Subscribed aSubscribeEvent) {
                super.onSubscribe(aSubscribeEvent);

                ForkJoinPool.commonPool().submit(

                        () -> {

                            try {
                                int failures = 0;

                                while (mSchedulerRunningState.get().equals(RunningState.RUNNING)) {
                                    final Duration duration = Duration.between(this.getLastHeartbeat() != null ? this.getLastHeartbeat() : LocalDateTime.now(), LocalDateTime.now());
                                    final int heartbeatInterval = this.getHeartbeatInterval();

                                    if (duration.getSeconds() > heartbeatInterval + mSchedulerConfig.getHeartbeatDelaySeconds()) {
                                        failures++;

                                    } else {
                                        failures = 0;
                                    }

                                    if (failures > 1) {
                                        log.error("Missed 2 heartbeat intervals.  Triggering reconnection to masters");
                                        this.onHeartbeatFail();
                                        return;
                                    }

                                    Thread.sleep(heartbeatInterval * 1000);
                                }

                            } catch (InterruptedException aE) {
                                log.error(aE.getMessage(), aE);
                            }

                        }

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
    public synchronized VelocityTask launch(TaskDefinition aTaskDefinition) throws VelocityTaskException {
        mMeterRegistry.counter("velocity.counter.scheduler.taskLaunch").increment();

        final VelocityTask task = VelocityTask.builder()
                .taskDefinition(aTaskDefinition)
                .created(LocalDateTime.now())
                .build();

        if (mTaskRepository.getNumQueuedTasks() == 0) {
            mMesosScheduler.getRemote().revive(Collections.emptyList());
        }

        mTaskRepository.queueTask(task);

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
            return queuedTasks.stream().collect(Collectors.toMap(task -> task.getTaskInfo().getTaskId().getValue(), task -> task));

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

    private void stop() {
        mSchedulerRunningState.set(RunningState.STOPPED);
        mRunning = false;
        mReconnectTask.cancel(true);
    }

    public void drainAndClose() throws Exception {
        waitTillEmpty();
        log.info("Scheduler is empty.  Now closing.");
        close();
    }

    private void waitTillEmpty() throws InterruptedException {
        int numActiveTasks = getNumActiveTasks();
        int numQueuedTasks = getNumQueuedTasks();

        while (numActiveTasks > 0 && numQueuedTasks > 0) {
            log.info(String.format("Waiting on task completion.  #Queued: %d, #Active: %d.", numQueuedTasks, numActiveTasks));

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

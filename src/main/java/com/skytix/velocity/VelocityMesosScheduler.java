package com.skytix.velocity;

import com.skytix.schedulerclient.Scheduler;
import com.skytix.velocity.entities.TaskDefinition;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.repository.InMemoryTaskRepository;
import com.skytix.velocity.repository.TaskRepository;
import com.skytix.velocity.scheduler.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.scheduler.Protos;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class VelocityMesosScheduler implements MesosScheduler {
    private Scheduler mMesosScheduler;
    private boolean mRunning = true;

    private final TaskRepository<VelocityTask> mTaskRepository;
    private final MeterRegistry mMeterRegistry;
    private final VelocitySchedulerConfig mSchedulerConfig;
    private final TaskEventHandler mDefaultEventHandler;
    private final ForkJoinTask<?> mReconnectTask;
    private final Object mErrorMonitor = new Object();
    private final AtomicReference<RunningState> mSchedulerRunningState = new AtomicReference<>(RunningState.STOPPED);

    public VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig) {
        this(aSchedulerConfig, new SimpleMeterRegistry());
    }

    public VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig, TaskEventHandler aDefaultEventHandler) {
        this(aSchedulerConfig, new SimpleMeterRegistry(), aDefaultEventHandler);
    }

    public VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig, MeterRegistry aMeterRegistry, TaskEventHandler aDefaultEventHandler) {
        this(aSchedulerConfig, aMeterRegistry, new InMemoryTaskRepository(aMeterRegistry, aSchedulerConfig), aDefaultEventHandler);
    }

    public VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig, MeterRegistry aMeterRegistry) {
        this(aSchedulerConfig, aMeterRegistry, new InMemoryTaskRepository(aMeterRegistry, aSchedulerConfig), null);
    }

    private VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig, MeterRegistry aMeterRegistry, TaskRepository<VelocityTask> aTaskRepository, TaskEventHandler aDefaultEventHandler) {
        mSchedulerConfig = aSchedulerConfig;
        mMeterRegistry = aMeterRegistry;
        mTaskRepository = aTaskRepository;
        mDefaultEventHandler = aDefaultEventHandler;

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
                mDefaultEventHandler,
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
                    aE.printStackTrace();
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

    @Override
    public void close() throws IOException {
        mSchedulerRunningState.set(RunningState.STOPPED);
        mRunning = false;
        mReconnectTask.cancel(true);
        mMesosScheduler.close();
    }

}

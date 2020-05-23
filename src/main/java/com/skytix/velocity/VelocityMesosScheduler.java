package com.skytix.velocity;

import com.skytix.schedulerclient.Scheduler;
import com.skytix.velocity.entities.TaskDefinition;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.repository.InMemoryTaskRepository;
import com.skytix.velocity.repository.TaskRepository;
import com.skytix.velocity.scheduler.MesosScheduler;
import com.skytix.velocity.scheduler.TaskEventHandler;
import com.skytix.velocity.scheduler.VelocitySchedulerConfig;
import com.skytix.velocity.scheduler.VelocitySchedulerHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDateTime;

@Slf4j
public class VelocityMesosScheduler implements MesosScheduler {
    private final Scheduler mMesosScheduler;
    private final TaskRepository<VelocityTask> mTaskRepository;
    private final MeterRegistry mMeterRegistry;

    public VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig) throws Exception {
        this(aSchedulerConfig, new SimpleMeterRegistry());
    }

    public VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig, TaskEventHandler aDefaultEventHandler) throws Exception {
        this(aSchedulerConfig, new SimpleMeterRegistry(), aDefaultEventHandler);
    }

    public VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig, MeterRegistry aMeterRegistry, TaskEventHandler aDefaultEventHandler) throws Exception {
        this(aSchedulerConfig, aMeterRegistry, new InMemoryTaskRepository(aMeterRegistry, aSchedulerConfig), aDefaultEventHandler);
    }

    public VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig, MeterRegistry aMeterRegistry) throws Exception {
        this(aSchedulerConfig, aMeterRegistry, new InMemoryTaskRepository(aMeterRegistry, aSchedulerConfig), null);
    }

    private VelocityMesosScheduler(VelocitySchedulerConfig aSchedulerConfig, MeterRegistry aMeterRegistry, TaskRepository<VelocityTask> aTaskRepository, TaskEventHandler aDefaultEventHandler) throws IOException {
        mTaskRepository = aTaskRepository;
        mMeterRegistry = aMeterRegistry;

        mMesosScheduler = Scheduler.newScheduler(
                aSchedulerConfig,
                new VelocitySchedulerHandler(
                        aTaskRepository,
                        aDefaultEventHandler,
                        aMeterRegistry,
                        aSchedulerConfig
                )
        );

    }

    @Override
    public synchronized VelocityTask launch(TaskDefinition aTaskDefinition) throws VelocityTaskException {
        mMeterRegistry.counter("velocity.counter.scheduler.taskLaunch").increment();

        final VelocityTask task = VelocityTask.builder()
                .taskDefinition(aTaskDefinition)
                .created(LocalDateTime.now())
                .build();

        mTaskRepository.queueTask(task);

        return task;
    }

    @Override
    public void close() throws IOException {
        mMesosScheduler.close();
    }

}

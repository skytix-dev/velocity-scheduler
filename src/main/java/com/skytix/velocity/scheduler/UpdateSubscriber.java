package com.skytix.velocity.scheduler;

import com.skytix.velocity.VelocityTaskException;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.repository.TaskRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.scheduler.Protos.Event.Update;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

@Slf4j
public class UpdateSubscriber implements Flow.Subscriber<Update> {
    private final TaskRepository<VelocityTask> mTaskRepository;
    private final SubmissionPublisher<TaskUpdateEvent> mEventUpdatePublisher;
    private final SchedulerRemoteProvider mRemote;
    private final TaskEventHandler mDefaultUpdateHandler;

    private final MeterRegistry mMeterRegistry;
    private final Counter mCompletedTasksCounter;
    private final Timer mTaskDurationTimer;
    private final Timer mTaskTotalDurationTimer;
    private final Timer mTaskQueuedDurationTimer;
    private final Counter mRetriedTasksCounter;
    private final Counter mFailedTasksCounter;

    private Flow.Subscription mSubscription;

    public UpdateSubscriber(TaskRepository<VelocityTask> aTaskRepository, SubmissionPublisher<TaskUpdateEvent> aSubmissionPublisher, SchedulerRemoteProvider aRemote, TaskEventHandler aDefaultUpdateHandler, MeterRegistry aMeterRegistry) {
        mTaskRepository = aTaskRepository;
        mEventUpdatePublisher = aSubmissionPublisher;
        mRemote = aRemote;
        mDefaultUpdateHandler = aDefaultUpdateHandler;

        mMeterRegistry = aMeterRegistry;
        mCompletedTasksCounter = mMeterRegistry.counter("velocity.counter.scheduler.completedTasks");
        mTaskDurationTimer = mMeterRegistry.timer("velocity.timer.scheduler.taskDuration");
        mTaskTotalDurationTimer = mMeterRegistry.timer("velocity.timer.scheduler.taskTotalDuration");
        mTaskQueuedDurationTimer = mMeterRegistry.timer("velocity.timer.scheduler.taskQueuedDuration");
        mRetriedTasksCounter = mMeterRegistry.counter("velocity.counter.scheduler.retriedTasks");
        mFailedTasksCounter = mMeterRegistry.counter("velocity.counter.scheduler.failedTasks");

        mEventUpdatePublisher.subscribe(new TaskEventUpdateSubscriber(aDefaultUpdateHandler));
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        mSubscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(Update update) {

        try {
            final Protos.TaskStatus updateStatus = update.getStatus();
            final VelocityTask task = mTaskRepository.getTaskByTaskId(updateStatus.getTaskId().getValue());

            if (task != null) {
                mTaskRepository.updateTaskState(task, updateStatus.getState());
                // Send acknowledgement first to prevent handlers from delaying the potential release of resources.
                acknowledge(updateStatus);

                switch (updateStatus.getState()) {

                    case TASK_RUNNING:

                        if (!task.isRunning()) {
                            task.setRunning(true);
                            task.setStartTime(LocalDateTime.now());

                            mTaskQueuedDurationTimer.record(Duration.between(task.getCreated(), LocalDateTime.now()));
                        }

                        break;

                    case TASK_FINISHED:

                        if (!task.isComplete()) {
                            task.setFinishTime(LocalDateTime.now());

                            mTaskRepository.completeTask(task);
                            mCompletedTasksCounter.increment();

                            recordTaskDuration(task);

                            suppressOffersIfIdle();
                        }

                        break;

                    case TASK_DROPPED:
                    case TASK_FAILED:
                    case TASK_ERROR:
                    case TASK_KILLED:
                    case TASK_GONE:
                    case TASK_GONE_BY_OPERATOR:
                    case TASK_LOST:
                        task.setFinishTime(LocalDateTime.now());
                        recordTaskDuration(task);

                        switch (updateStatus.getReason()) {
                            case REASON_CONTAINER_LAUNCH_FAILED:
                            case REASON_TASK_KILLED_DURING_LAUNCH:
                            case REASON_EXECUTOR_TERMINATED:
                            case REASON_GC_ERROR:
                            case REASON_INVALID_OFFERS:
                                // Retry the task since it may be ephemeral.
                                try {
                                    log.debug(String.format("Task %s failed for reason: %s. Retrying...", updateStatus.getTaskId(), updateStatus.getReason()));
                                    mRetriedTasksCounter.increment();

                                    mTaskRepository.retryTask(task);

                                    if (mTaskRepository.getNumQueuedTasks() > 0) { // If we just ran 1 task, the queue will be empty so we will want to revive the offers.
                                        mRemote.get().revive(Collections.emptyList());
                                    }

                                } catch (VelocityTaskException aE) {
                                    log.error(aE.getMessage(), aE);
                                }

                                break;

                            default:
                                mFailedTasksCounter.increment();
                                log.debug(String.format("Task %s failed for reason: (%s) %s.", updateStatus.getTaskId(), updateStatus.getReason(), updateStatus.getMessage()));
                                mTaskRepository.completeTask(task);
                                break;
                        }

                        suppressOffersIfIdle();

                        break;
                }

                mEventUpdatePublisher.submit(
                        TaskUpdateEvent.builder()
                                .event(update)
                                .task(task)
                                .build()
                );

            } else {
                // We don't know about the task anymore so acknowledge the updates.
                acknowledge(updateStatus);

                if (mDefaultUpdateHandler != null) {
                    mDefaultUpdateHandler.onEvent(update);
                }

            }

        } catch (Exception aE) {
            log.error(aE.getMessage(), aE);

        } finally {
            mSubscription.request(1);
        }

    }

    private void suppressOffersIfIdle() {
        if (mTaskRepository.getNumQueuedTasks() == 0 && mTaskRepository.getNumActiveTasks() == 0) {
            log.debug("Scheduler is idle. Suppressing offers");
            mRemote.get().suppress(Collections.emptyList());
        }
    }

    private void recordTaskDuration(VelocityTask aTask) {
        mTaskDurationTimer.record(
                Duration.between(
                        aTask.getStartTime() != null ? aTask.getStartTime() : aTask.getCreated(),
                        aTask.getFinishTime() != null ? aTask.getFinishTime() : LocalDateTime.now())
        );

        mTaskTotalDurationTimer.record(
                Duration.between(
                        aTask.getCreated(),
                        LocalDateTime.now())
        );
    }

    private void acknowledge(Protos.TaskStatus aStatus) {

        if (aStatus.hasUuid()) {
            mRemote.get().acknowledge(aStatus);
        }

    }

    @Override
    public void onError(Throwable throwable) {
        log.error(throwable.getMessage(), throwable);
    }

    @Override
    public void onComplete() {
        // Yay?
    }

}

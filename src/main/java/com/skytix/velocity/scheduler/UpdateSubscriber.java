package com.skytix.velocity.scheduler;

import com.skytix.velocity.VelocityTaskException;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.repository.TaskRepository;
import io.micrometer.core.instrument.MeterRegistry;
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

    private Flow.Subscription mSubscription;

    public UpdateSubscriber(TaskRepository<VelocityTask> aTaskRepository, SubmissionPublisher<TaskUpdateEvent> aSubmissionPublisher, SchedulerRemoteProvider aRemote, TaskEventHandler aDefaultUpdateHandler, MeterRegistry aMeterRegistry) {
        mTaskRepository = aTaskRepository;
        mEventUpdatePublisher = aSubmissionPublisher;
        mRemote = aRemote;
        mDefaultUpdateHandler = aDefaultUpdateHandler;
        mMeterRegistry = aMeterRegistry;

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

                // Do some stuff with the task.
                switch (updateStatus.getState()) {

                    case TASK_RUNNING:

                        if (!task.isRunning()) {
                            task.setRunning(true);
                            task.setStartTime(LocalDateTime.now());
                        }

                        break;

                    case TASK_FINISHED:

                        if (!task.isComplete()) {
                            task.setFinishTime(LocalDateTime.now());

                            mTaskRepository.completeTask(task);
                            mMeterRegistry.counter("velocity.counter.scheduler.completedTasks").increment();
                            mMeterRegistry.timer("velocity.timer.scheduler.taskDuration").record(Duration.between(task.getStartTime(), task.getFinishTime()));

                            if (mTaskRepository.getNumQueuedTasks() == 0 && mTaskRepository.getNumActiveTasks() == 0) {
                                log.debug("Scheduler is idle. Suppressing offers");
                                mRemote.get().suppress(Collections.emptyList());
                            }

                        }

                        break;

                    case TASK_DROPPED:
                    case TASK_FAILED:
                    case TASK_ERROR:
                    case TASK_KILLED:
                    case TASK_GONE:
                    case TASK_GONE_BY_OPERATOR:
                    case TASK_LOST:

                        switch (updateStatus.getReason()) {
                            case REASON_CONTAINER_LAUNCH_FAILED:
                            case REASON_TASK_KILLED_DURING_LAUNCH:
                            case REASON_EXECUTOR_TERMINATED:
                            case REASON_GC_ERROR:
                            case REASON_INVALID_OFFERS:
                                // Retry the task since it may be ephemeral.
                                try {
                                    log.debug(String.format("Task %s failed for reason: %s. Retrying...", updateStatus.getTaskId(), updateStatus.getReason()));
                                    mMeterRegistry.counter("velocity.counter.scheduler.retriedTasks").increment();
                                    mTaskRepository.retryTask(task);

                                } catch (VelocityTaskException aE) {
                                    log.error(aE.getMessage(), aE);
                                }

                                break;

                            default:
                                mMeterRegistry.counter("velocity.counter.scheduler.failedTasks").increment();
                                log.debug(String.format("Task %s failed for reason: (%s) %s.", updateStatus.getTaskId(), updateStatus.getReason(), updateStatus.getMessage()));
                                task.setFinishTime(LocalDateTime.now());
                                mTaskRepository.completeTask(task);
                                break;
                        }

                        break;
                }

                // In the event that a scheduler needs to reconnect, it may get take UPDATE messages from tasks it
                // no longer knows about so the default update handler will be invoked if it's defined.
                final TaskEventHandler taskEventHandler = task.getTaskDefinition().getTaskEventHandler();

                mEventUpdatePublisher.submit(
                        TaskUpdateEvent.builder()
                                .event(update)
                                .task(task)
                                .build()
                );

                if (taskEventHandler != null) {
                    taskEventHandler.onEvent(update);

                } else if (mDefaultUpdateHandler != null) {
                    mDefaultUpdateHandler.onEvent(update);
                }

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

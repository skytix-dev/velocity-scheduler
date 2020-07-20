package com.skytix.velocity.scheduler;

import com.skytix.velocity.VelocityTaskException;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.repository.TaskRepository;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Flow;

@Slf4j
public class TaskSubscriber implements Flow.Subscriber<VelocityTask> {
    private final TaskRepository<VelocityTask> mTaskRepository;
    private Flow.Subscription mSubscription;

    public TaskSubscriber(TaskRepository<VelocityTask> aTaskRepository) {
        mTaskRepository = aTaskRepository;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        mSubscription = subscription;
        mSubscription.request(1);
    }

    @Override
    public void onNext(VelocityTask item) {

        try {
            mTaskRepository.queueTask(item);

        } catch (VelocityTaskException e) {
            log.error(e.getMessage(), e);

        } finally {
            mSubscription.request(1);
        }

    }

    @Override
    public void onError(Throwable throwable) {
        log.error(throwable.getMessage(), throwable);
    }

    @Override
    public void onComplete() {

    }
}

package com.skytix.velocity.scheduler;

import com.skytix.velocity.entities.VelocityTask;
import org.apache.mesos.v1.scheduler.Protos;

import java.util.concurrent.Flow;

public class TaskEventUpdateSubscriber implements Flow.Subscriber<TaskUpdateEvent> {
    private final TaskEventHandler mDefaultUpdateHandler;
    private Flow.Subscription mSubscription;

    public TaskEventUpdateSubscriber(TaskEventHandler aDefaultUpdateHandler) {
        mDefaultUpdateHandler = aDefaultUpdateHandler;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        mSubscription = subscription;
        mSubscription.request(1);
    }

    @Override
    public void onNext(TaskUpdateEvent item) {
        final VelocityTask task = item.getTask();
        final Protos.Event.Update event = item.getEvent();

        try {
            if (task != null) {
                final TaskEventHandler taskEventHandler = task.getTaskDefinition().getTaskEventHandler();

                if (taskEventHandler != null) {
                    taskEventHandler.onEvent(event);

                } else if (mDefaultUpdateHandler != null) {
                    mDefaultUpdateHandler.onEvent(event);
                }

            } else {

                if (mDefaultUpdateHandler != null) {
                    mDefaultUpdateHandler.onEvent(event);
                }

            }

        } finally {
            mSubscription.request(1);
        }

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }
}

package com.skytix.velocity.scheduler;

import com.skytix.schedulerclient.BaseSchedulerEventHandler;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.repository.TaskRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.apache.mesos.v1.scheduler.Protos.Event.Offers;

import java.util.Collections;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;

@Slf4j
public class VelocitySchedulerHandler extends BaseSchedulerEventHandler {


    private final SubmissionPublisher<Protos.Offer> mOfferPublisher;
    private final SubmissionPublisher<Event.Update> mUpdatePublisher;

    private final TaskRepository<VelocityTask> mTaskRepository;
    private final TaskEventHandler mDefaultEventHandler;
    private final MeterRegistry mMeterRegistry;

    public VelocitySchedulerHandler(TaskRepository<VelocityTask> aTaskRepository, TaskEventHandler aDefaultEventHandler, MeterRegistry aMeterRegistry, VelocitySchedulerConfig aConfig) {
        mTaskRepository = aTaskRepository;
        mDefaultEventHandler = aDefaultEventHandler;
        mMeterRegistry = aMeterRegistry;

        final Integer maxOfferQueueSize = aConfig.getMaxOfferQueueSize();
        final Integer maxUpdateQueueSize = aConfig.getMaxUpdateQueueSize();

        if (maxOfferQueueSize <= 0) {
            throw new IllegalArgumentException("maxOfferQueueSize must be greater than zero");
        }

        if (maxUpdateQueueSize <= 0) {
            throw new IllegalArgumentException("maxUpdateQueueSize must be create than zero");
        }

        mOfferPublisher = new SubmissionPublisher<>(ForkJoinPool.commonPool(), maxOfferQueueSize);
        mUpdatePublisher = new SubmissionPublisher<>(ForkJoinPool.commonPool(), maxUpdateQueueSize);
    }

    @Override
    public void onSubscribe() {
        mOfferPublisher.subscribe(new OfferSubscriber(mTaskRepository, getSchedulerRemote(), mMeterRegistry));
        mUpdatePublisher.subscribe(new UpdateSubscriber(mTaskRepository, getSchedulerRemote(), mDefaultEventHandler, mMeterRegistry));

        // This will cause Mesos to send updates for all non-terminal tasks.
        getSchedulerRemote().reconcile(Collections.emptyList());
    }

    @Override
    public void handleEvent(Event aEvent) throws Exception {

        switch (aEvent.getType()) {

            case OFFERS:
                final Offers offers = aEvent.getOffers();

                for (int i = 0; i < offers.getOffersCount(); i++) {

                    mOfferPublisher.offer(offers.getOffers(i), 2, TimeUnit.SECONDS, (subscriber, offer) -> {
                        log.error(String.format("Timeout adding offer '%s' to queue.  Queue full.  Declining offer.", offer.getId().getValue()));
                        getSchedulerRemote().decline(Collections.singletonList(offer.getId()));
                        return false;
                    });

                }

                break;

            case RESCIND:
                handleRescind(aEvent.getRescind());
                break;

            case UPDATE:

                mUpdatePublisher.offer(aEvent.getUpdate(), 2, TimeUnit.SECONDS, (subscriber, update) -> {
                    log.error(String.format("Timeout adding update for task '%s' to queue.  Queue full.  Retrying...", update.getStatus().getTaskId()));
                    return true;
                });

                break;

        }

    }

    private void handleRescind(Event.Rescind aRescind) {
        /* Since we either launch tasks or reject offers, there shouldn't be any offers on hold to rescind.  However if
         * we receive an offer and start finding tasks for it and a rescind request is received, we need to cancel it.
         */
    }


}

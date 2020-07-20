package com.skytix.velocity.scheduler;

import com.skytix.schedulerclient.BaseSchedulerEventHandler;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.repository.TaskRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.scheduler.Protos.Call.Reconcile.Task;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.apache.mesos.v1.scheduler.Protos.Event.Offers;
import org.apache.mesos.v1.scheduler.Protos.Event.Subscribed;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class VelocitySchedulerHandler extends BaseSchedulerEventHandler {
    private final SubmissionPublisher<Protos.Offer> mOfferPublisher;
    private final SubmissionPublisher<Event.Update> mUpdatePublisher;
    private final SubmissionPublisher<TaskUpdateEvent> mTaskUpdatePublisher;

    private final TaskRepository<VelocityTask> mTaskRepository;
    private final MeterRegistry mMeterRegistry;
    private final VelocitySchedulerConfig mSchedulerConfig;

    private LocalDateTime mLastHeartbeat = null;
    private int mHeartbeatInterval = 0;

    public VelocitySchedulerHandler(TaskRepository<VelocityTask> aTaskRepository, MeterRegistry aMeterRegistry, VelocitySchedulerConfig aConfig) {
        mTaskRepository = aTaskRepository;
        mMeterRegistry = aMeterRegistry;
        mSchedulerConfig = aConfig;

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
        mTaskUpdatePublisher = new SubmissionPublisher<>(ForkJoinPool.commonPool(), 1000);
    }

    @Override
    public void onSubscribe(Subscribed aSubscribeEvent) {
        mOfferPublisher.subscribe(new OfferSubscriber(mTaskRepository, this::getSchedulerRemote, mMeterRegistry));
        mUpdatePublisher.subscribe(new UpdateSubscriber(mTaskRepository, mTaskUpdatePublisher, this::getSchedulerRemote, mSchedulerConfig.getDefaultTaskEventHandler(), mMeterRegistry));

        // Get Mesos to send status updates for tasks that were previously running.
        getSchedulerRemote().reconcile(buildFromRunningTasks());

        mHeartbeatInterval = (int) aSubscribeEvent.getHeartbeatIntervalSeconds();
    }

    @Override
    public void handleEvent(Event aEvent) {

        switch (aEvent.getType()) {

            case INVERSE_OFFERS:
                // We currently don't support inverse offers just yet.  Tasks will one day be able to declare if they can be killed and moved.
                final Event.InverseOffers inverseOffers = aEvent.getInverseOffers();

                for (int i = 0; i < inverseOffers.getInverseOffersCount(); i++) {
                    final Protos.InverseOffer inverseOffer = inverseOffers.getInverseOffers(i);
                    getSchedulerRemote().decline(Collections.singletonList(inverseOffer.getId()));
                }

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

            case HEARTBEAT:
                mLastHeartbeat = LocalDateTime.now();

                if (mSchedulerConfig.getHeartbeatListener() != null) {
                    mSchedulerConfig.getHeartbeatListener().beat();
                }

                break;

        }

    }

    public LocalDateTime getLastHeartbeat() {
        return mLastHeartbeat;
    }

    public int getHeartbeatInterval() {
        return mHeartbeatInterval;
    }

    private void handleRescind(Event.Rescind aRescind) {
        /* Since we either launch tasks or reject offers, there shouldn't be any offers on hold to rescind.  However if
         * we receive an offer and start finding tasks for it and a rescind request is received, we need to cancel it.
         */
    }

    private List<Task> buildFromRunningTasks() {
        final List<VelocityTask> activeTasks = mTaskRepository.getActiveTasks();
        final List<Task> results = new ArrayList<>(activeTasks.size());

        activeTasks.forEach((aVelocityTask -> {
            results.add(
                    Task.newBuilder()
                            .setTaskId(aVelocityTask.getTaskInfo().getTaskId())
                            .setAgentId(aVelocityTask.getTaskInfo().getAgentId())
                            .build()
            );

        }));

        return results;
    }

    @Override
    public void onTerminate(Exception aException) {
        onDisconnect();
    }

    public abstract void onHeartbeatFail();

    @Override
    public void onDisconnect() {
        mOfferPublisher.close();
        mUpdatePublisher.close();
    }

}

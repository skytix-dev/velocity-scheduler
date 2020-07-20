package com.skytix.velocity.scheduler;

import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.repository.TaskRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.Protos;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Flow;

@Slf4j
public class OfferSubscriber implements Flow.Subscriber<Protos.Offer> {
    private final TaskRepository<VelocityTask> mTaskRepository;
    private final SchedulerRemoteProvider mRemote;
    private final MeterRegistry mMeterRegistry;
    private Flow.Subscription mSubscription;


    public OfferSubscriber(TaskRepository<VelocityTask> aTaskRepository, SchedulerRemoteProvider aRemote, MeterRegistry aMeterRegistry) {
        mTaskRepository = aTaskRepository;
        mRemote = aRemote;
        mMeterRegistry = aMeterRegistry;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        mSubscription = subscription;
        mSubscription.request(1);
    }

    @Override
    public void onNext(Protos.Offer offer) {

        try {
            final Protos.OfferID offerId = offer.getId();

            if (isAvailable(offer)) {
                final List<Protos.TaskInfo.Builder> matchingTasks = mTaskRepository.getMatchingWaitingTasks(offer);

                if (matchingTasks.size() > 0) {
                    final Protos.Offer.Operation.Builder operation = buildLaunchOperation(offer, matchingTasks);
                    final List<Protos.TaskInfo> infoList = operation.getLaunch().getTaskInfosList();
                    final int numTasks = infoList.size();

                    mMeterRegistry.counter("velocity.counter.scheduler.offerTasksLaunched").increment(numTasks);

                    mTaskRepository.launchTasks(infoList);

                    for (Protos.TaskInfo taskInfo : infoList) {
                        final VelocityTask task = mTaskRepository.getTaskByTaskId(taskInfo.getTaskId().getValue());

                        if (task != null) {
                            task.setState(Protos.TaskState.TASK_STAGING);
                            task.setRemote(VelocityTaskRemote.builder()
                                    .schedulerRemote(mRemote)
                                    .agentID(taskInfo.getAgentId())
                                    .taskID(taskInfo.getTaskId())
                                    .build()
                            );

                        }

                    }

                    mRemote.get().accept(
                            Collections.singletonList(offer.getId()),
                            Collections.singletonList(operation.build())
                    );

                } else {
                    mRemote.get().decline(Collections.singletonList(offerId));
                }

            } else {
                mRemote.get().decline(Collections.singletonList(offerId));
            }

        } catch (Exception aE) {
            log.error(aE.getMessage(), aE);

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

    private boolean isAvailable(Protos.Offer aOffer) {
        final Protos.Unavailability unavailabilityInfo = aOffer.getUnavailability();

        if (unavailabilityInfo != null) {
            final LocalDateTime now = LocalDateTime.now();
            final LocalDateTime start = LocalDateTime.from(Instant.ofEpochMilli(unavailabilityInfo.getStart().getNanoseconds() / 1000000));

            // No duration means maintenance lasts forever.
            if (unavailabilityInfo.getDuration() != null) {
                final Duration duration = Duration.of(unavailabilityInfo.getDuration().getNanoseconds(), ChronoUnit.NANOS);
                final LocalDateTime end = start.plus(duration);

                return now.isBefore(start) || now.isAfter(end) || now.isEqual(end);

            } else {
                return now.isBefore(start);
            }

        } else {
            return true;
        }

    }

    private Protos.Offer.Operation.Builder buildLaunchOperation(Protos.Offer aOffer, List<Protos.TaskInfo.Builder> aTasks) {
        final Protos.Offer.Operation.Launch.Builder launch = Protos.Offer.Operation.Launch.newBuilder();

        for (Protos.TaskInfo.Builder taskInfo : aTasks) {
            launch.addTaskInfos(taskInfo.setAgentId(aOffer.getAgentId()));
        }

        return Protos.Offer.Operation.newBuilder()
                .setType(Protos.Offer.Operation.Type.LAUNCH)
                .setLaunch(launch);

    }

}

package com.skytix.velocity.scheduler;

import com.skytix.schedulerclient.SchedulerRemote;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.repository.TaskRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.Protos;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Flow;

@Slf4j
public class OfferSubscriber implements Flow.Subscriber<Protos.Offer> {
    private final TaskRepository<VelocityTask> mTaskRepository;
    private final SchedulerRemote mRemote;
    private final MeterRegistry mMeterRegistry;
    private Flow.Subscription mSubscription;


    public OfferSubscriber(TaskRepository<VelocityTask> aTaskRepository, SchedulerRemote aRemote, MeterRegistry aMeterRegistry) {
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

            mRemote.accept(
                    Collections.singletonList(offer.getId()),
                    Collections.singletonList(operation.build())
            );

        } else {
            final Protos.OfferID offerId = offer.getId();
            mRemote.decline(Collections.singletonList(offerId));
        }

        mSubscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

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

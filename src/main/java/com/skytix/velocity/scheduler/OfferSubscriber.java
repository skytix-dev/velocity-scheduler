package com.skytix.velocity.scheduler;

import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.repository.TaskRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.Protos;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
    private final Counter mOfferTasksLaunchedCounter;


    public OfferSubscriber(TaskRepository<VelocityTask> aTaskRepository, SchedulerRemoteProvider aRemote, MeterRegistry aMeterRegistry) {
        mTaskRepository = aTaskRepository;
        mRemote = aRemote;
        mMeterRegistry = aMeterRegistry;

        mOfferTasksLaunchedCounter = mMeterRegistry.counter("velocity.counter.scheduler.offerTasksLaunched");
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

                    mOfferTasksLaunchedCounter.increment(numTasks);

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

        if (unavailabilityInfo.isInitialized()) {
            final LocalDateTime now = LocalDateTime.now();
            final Protos.TimeInfo startInfo = unavailabilityInfo.getStart();

            if (startInfo.isInitialized()) {
                final LocalDateTime start = Instant.ofEpochMilli(startInfo.getNanoseconds() / 1000000).atZone(ZoneId.systemDefault()).toLocalDateTime();
                final Protos.DurationInfo durationInfo = unavailabilityInfo.getDuration();

                // No duration means maintenance lasts forever.
                if (durationInfo != null && durationInfo.isInitialized()) {
                    final Duration duration = Duration.of(durationInfo.getNanoseconds(), ChronoUnit.NANOS);
                    final LocalDateTime end = start.plus(duration);

                    return now.isBefore(start) || now.isAfter(end) || now.isEqual(end);

                } else {
                    return now.isBefore(start);
                }

            } else {
                return true;
            }

        } else {
            return true;
        }

    }

    private Protos.Offer.Operation.Builder buildLaunchOperation(Protos.Offer aOffer, List<Protos.TaskInfo.Builder> aTasks) {
        final Protos.Offer.Operation.Launch.Builder launch = Protos.Offer.Operation.Launch.newBuilder();

        for (Protos.TaskInfo.Builder taskInfo : aTasks) {
            setStandardTaskEnvVars(aOffer, taskInfo);
            launch.addTaskInfos(taskInfo.setAgentId(aOffer.getAgentId()));
        }

        return Protos.Offer.Operation.newBuilder()
                .setType(Protos.Offer.Operation.Type.LAUNCH)
                .setLaunch(launch);

    }

    private void setStandardTaskEnvVars(Protos.Offer aOffer, Protos.TaskInfo.Builder aTaskInfo) {

        if (!aTaskInfo.hasCommand()) {
            aTaskInfo.setCommand(Protos.CommandInfo.newBuilder());
        }

        final Protos.CommandInfo.Builder commandBuilder = aTaskInfo.getCommandBuilder();

        if (!commandBuilder.hasEnvironment()) {
            commandBuilder.setEnvironment(Protos.Environment.newBuilder());
        }

        final Protos.Environment.Builder environmentBuilder = commandBuilder.getEnvironmentBuilder();

        removeEnvVar("HOST", environmentBuilder);
        removeEnvVar("MESOS_TASK_ID", environmentBuilder);

        environmentBuilder.addVariables(Protos.Environment.Variable.newBuilder()
                .setName("HOST")
                .setValue(aOffer.getHostname())
        );

        environmentBuilder.addVariables(Protos.Environment.Variable.newBuilder()
                .setName("MESOS_TASK_ID")
                .setValue(aTaskInfo.getTaskId().getValue())
        );

        if (aTaskInfo.getContainer().getType() == Protos.ContainerInfo.Type.DOCKER) {
            final Protos.ContainerInfo.DockerInfo docker = aTaskInfo.getContainer().getDocker();

            switch (docker.getNetwork()) {

                case BRIDGE:

                    for (int i = 0; i < docker.getPortMappingsCount(); i++) {
                        final Protos.ContainerInfo.DockerInfo.PortMapping portMapping = docker.getPortMappings(i);

                        environmentBuilder.addVariables(
                                Protos.Environment.Variable.newBuilder()
                                        .setName(String.format("PORT%d", i))
                                        .setValue(Integer.toString(portMapping.getHostPort()))
                                        .build()
                        );

                    }

                    break;

                case HOST:
                    final Protos.Ports.Builder ports = aTaskInfo.getDiscoveryBuilder().getPortsBuilder();

                    for (int i = 0; i < ports.getPortsCount(); i++) {
                        final Protos.Port.Builder port = ports.getPortsBuilder(i);

                        environmentBuilder.addVariables(
                                Protos.Environment.Variable.newBuilder()
                                        .setName(String.format("PORT%d", i))
                                        .setValue(Integer.toString(port.getNumber()))
                                        .build()
                        );

                    }

                    break;
            }

        }

    }

    private void removeEnvVar(String aName, Protos.Environment.Builder aEnv) {

        for (int idx = 0; idx < aEnv.getVariablesCount(); idx++) {
            final Protos.Environment.Variable var = aEnv.getVariables(idx);

            if (var.getName().equals(aName)) {
                aEnv.removeVariables(idx);
                return;
            }

        }

    }

}

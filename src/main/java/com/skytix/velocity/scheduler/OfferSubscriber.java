package com.skytix.velocity.scheduler;

import com.skytix.schedulerclient.mesos.MesosConstants;
import com.skytix.velocity.entities.VelocityTask;
import com.skytix.velocity.mesos.MesosUtils;
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
import java.util.Set;
import java.util.concurrent.Flow;

@Slf4j
public class OfferSubscriber implements Flow.Subscriber<Protos.Offer> {
    private final TaskRepository<VelocityTask> mTaskRepository;
    private final SchedulerRemoteProvider mRemote;
    private final MeterRegistry mMeterRegistry;
    private Flow.Subscription mSubscription;
    private final Counter mOfferTasksLaunchedCounter;
    private Protos.OfferFilters mDefaultOfferFilters;
    private Protos.OfferFilters mCurrentOfferFilters;


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

        final Protos.FrameworkInfo frameworkInfo = mRemote.get().getFrameworkInfo();

        if (frameworkInfo.getOfferFiltersCount() > 0) {
            mDefaultOfferFilters = frameworkInfo.getOfferFiltersMap().get(MesosConstants.ROLE_ALL);
        }

    }

    @Override
    public void onNext(Protos.Offer offer) {

        try {
            final Protos.OfferID offerId = offer.getId();

            if (isAvailable(offer)) {
                final List<VelocityTask> matchingTasks = mTaskRepository.getMatchingWaitingTasks(offer);

                if (matchingTasks.size() > 0) {
                    final Protos.Offer.Operation.Builder operation = buildLaunchOperation(offer, matchingTasks);
                    final List<Protos.TaskInfo> infoList = operation.getLaunch().getTaskInfosList();
                    final int numTasks = infoList.size();

                    mOfferTasksLaunchedCounter.increment(numTasks);

                    mTaskRepository.launchTasks(matchingTasks);

                    matchingTasks.forEach((task) -> {
                        final Protos.TaskInfo.Builder taskInfo = task.getTaskDefinition().getTaskInfo();

                        task.setState(Protos.TaskState.TASK_STAGING);

                        task.setRemote(VelocityTaskRemote.builder()
                                .schedulerRemote(mRemote)
                                .agentID(taskInfo.getAgentId())
                                .taskID(taskInfo.getTaskId())
                                .build()
                        );
                    });

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

            // Where jobs are missed because offers didn't contain enough resources, update the filters for the framework
            // to ensure correctly sized offers are provided.
            updateOfferFilters();

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

    private void updateOfferFilters() {
        final Set<VelocityTask> missedTasks = mTaskRepository.getMissedTasks();

        if (missedTasks.size() > 0) {
            double maxCpu = 0.0;
            double maxMem = 0.0;
            double maxGpu = 0.0;
            double maxDisk = 0.0;

            for (VelocityTask task : missedTasks) {
                final Protos.TaskInfoOrBuilder taskInfo = task.getTaskDefinition().getTaskInfo();

                final double cpus = MesosUtils.getCpus(taskInfo, 0.0);
                final double mem = MesosUtils.getMem(taskInfo, 0.0);
                final double gpus = MesosUtils.getGpus(taskInfo, 0.0);
                final double disk = MesosUtils.getDisk(taskInfo, 0.0);

                if (cpus > maxCpu) {
                    maxCpu = cpus;
                }

                if (mem > maxMem) {
                    maxMem = mem;
                }

                if (gpus > maxGpu) {
                    maxGpu = gpus;
                }

                if (disk > maxDisk) {
                    maxDisk = disk;
                }

            }

            final double minCpu;
            final double minMem;
            final double minGpu;
            final double minDisk;

            if (mCurrentOfferFilters != null) {
                final Protos.OfferFilters.MinAllocatableResources minAllocatableResources = mCurrentOfferFilters.getMinAllocatableResources();

                minCpu = MesosUtils.getCpus(minAllocatableResources, 0.0);
                minMem = MesosUtils.getMem(minAllocatableResources, 0.0);
                minGpu = MesosUtils.getGpus(minAllocatableResources, 0.0);
                minDisk = MesosUtils.getDisk(minAllocatableResources, 0.0);

            } else if (mDefaultOfferFilters != null) {
                final Protos.OfferFilters.MinAllocatableResources minAllocatableResources = mDefaultOfferFilters.getMinAllocatableResources();

                minCpu = MesosUtils.getCpus(minAllocatableResources, 0.0);
                minMem = MesosUtils.getMem(minAllocatableResources, 0.0);
                minGpu = MesosUtils.getGpus(minAllocatableResources, 0.0);
                minDisk = MesosUtils.getDisk(minAllocatableResources, 0.0);

            } else {
                minCpu = 0.0;
                minMem = 0.0;
                minGpu = 0.0;
                minDisk = 0.0;
            }

            if (maxCpu != minCpu || maxMem != minMem || maxGpu != minGpu || maxDisk != minDisk) {
                // We should update the filter. Else, just ignore it and not mess around calling the UPDATE_FRAMEWORK.
                final Protos.OfferFilters.MinAllocatableResources.Builder updatedResources = Protos.OfferFilters.MinAllocatableResources.newBuilder();

                updatedResources.addQuantities(Protos.OfferFilters.ResourceQuantities.newBuilder()
                        .putQuantities(
                                MesosConstants.SCALAR_CPU, Protos.Value.Scalar.newBuilder().setValue(maxCpu).build()
                        )
                );

                updatedResources.addQuantities(Protos.OfferFilters.ResourceQuantities.newBuilder()
                        .putQuantities(
                                MesosConstants.SCALAR_MEM, Protos.Value.Scalar.newBuilder().setValue(maxMem).build()
                        )
                );

                updatedResources.addQuantities(Protos.OfferFilters.ResourceQuantities.newBuilder()
                        .putQuantities(
                                MesosConstants.SCALAR_GPU, Protos.Value.Scalar.newBuilder().setValue(maxGpu).build()
                        )
                );

                updatedResources.addQuantities(Protos.OfferFilters.ResourceQuantities.newBuilder()
                        .putQuantities(
                                MesosConstants.SCALAR_DISK, Protos.Value.Scalar.newBuilder().setValue(maxDisk).build()
                        )
                );

                mCurrentOfferFilters = Protos.OfferFilters.newBuilder().setMinAllocatableResources(updatedResources).build();

                mRemote.get().updateFrameworkOfferFilters(mCurrentOfferFilters);
                log.debug(String.format("Updating Offer Filters to satisfy missed tasks: %s", updatedResources.toString()));
            }

        } else {

            if (mCurrentOfferFilters != null) {
                mRemote.get().resetFrameworkOfferFilters();
                mCurrentOfferFilters = null;
                log.debug("Setting Offer Filters back to defaults due to empty backlog");
            }

        }


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
                if (durationInfo.isInitialized()) {
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

    private Protos.Offer.Operation.Builder buildLaunchOperation(Protos.Offer aOffer, List<VelocityTask> aTasks) {
        final Protos.Offer.Operation.Launch.Builder launch = Protos.Offer.Operation.Launch.newBuilder();

        for (VelocityTask task : aTasks) {
            final Protos.TaskInfo.Builder taskInfo = task.getTaskDefinition().getTaskInfo();

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

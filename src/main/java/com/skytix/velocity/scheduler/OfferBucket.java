package com.skytix.velocity.scheduler;

import com.skytix.velocity.mesos.MesosUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.Protos;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo.PortMapping;

@Slf4j
public class OfferBucket {
    private final Protos.OfferID mOfferID;
    private final int mMaxTasksPerOffer;
    private final List<Protos.TaskInfo.Builder> mAllocatedTasks = new ArrayList<>();

    private final double mOfferCpus;
    private final double mOfferMem;
    private final double mOfferDisk;
    private final double mOfferGpus;

    private double mAllocatedCpus = 0.0;
    private double mAllocatedMem = 0.0;
    private double mAllocatedDisk = 0.0;
    private double mAllocatedGpus = 0.0;

    private final Queue<Integer> availablePortsQueue = new LinkedBlockingQueue<>();
    private final List<Integer> allAvailablePorts = new ArrayList<>();

    public OfferBucket(Protos.Offer aOffer) {
        this(aOffer, 15);
    }

    public OfferBucket(Protos.Offer aOffer, int aMaxTasksPerOffer) {
        mOfferID = aOffer.getId();
        mMaxTasksPerOffer = aMaxTasksPerOffer;

        mOfferCpus = MesosUtils.getCpus(aOffer, 0);
        mOfferMem = MesosUtils.getMem(aOffer, 0);
        mOfferDisk = MesosUtils.getDisk(aOffer, 0);
        mOfferGpus = MesosUtils.getGpus(aOffer, 0);

        final Protos.Resource ports = MesosUtils.getPorts(aOffer);

        if (ports != null) {

            for (Protos.Value.Range range : ports.getRanges().getRangeList()) {

                for (long idx = range.getBegin(); idx <= range.getEnd(); idx++) {
                    allAvailablePorts.add((int)idx);
                    availablePortsQueue.add((int)idx);
                }

            }

        }

    }

    public synchronized boolean hasResources(Protos.TaskInfoOrBuilder aTaskInfo) throws OfferBucketFullException {

        if (mAllocatedTasks.size() < mMaxTasksPerOffer) {

            if (mOfferCpus > 0 && mOfferMem > 0) {

                return hasCpuResources(aTaskInfo) &&
                        hasMemResources(aTaskInfo) &&
                        hasDiskResources(aTaskInfo) &&
                        hasGpuResources(aTaskInfo) &&
                        hasPortResources(aTaskInfo);

            } else {
                // If there's no cpu or mem available.  Then nothing more will run.
                throw new OfferBucketFullException();
            }

        } else {
            throw new OfferBucketFullException();
        }

    }

    public synchronized boolean hasCpuResources(Protos.TaskInfoOrBuilder aTaskInfo) {
        return mAllocatedCpus + MesosUtils.getCpus(aTaskInfo, 0) <= mOfferCpus;
    }

    public synchronized boolean hasGpuResources(Protos.TaskInfoOrBuilder aTaskInfo) {
        return mAllocatedGpus + MesosUtils.getGpus(aTaskInfo, 0) <= mOfferGpus;
    }

    public synchronized boolean hasMemResources(Protos.TaskInfoOrBuilder aTaskInfo) {
        return mAllocatedMem + MesosUtils.getMem(aTaskInfo, 0) <= mOfferMem;
    }

    public synchronized boolean hasDiskResources(Protos.TaskInfoOrBuilder aTaskInfo) {
        return mAllocatedDisk + MesosUtils.getDisk(aTaskInfo, 0) <= mOfferDisk;
    }

    public synchronized boolean hasPortResources(Protos.TaskInfoOrBuilder aTaskInfo) {
        final Protos.ContainerInfo container = aTaskInfo.getContainer();

        switch (container.getType()) {

            case DOCKER:
                // Check the docker port mappings.
                final Protos.ContainerInfo.DockerInfo docker = container.getDocker();

                switch (docker.getNetwork()) {

                    case BRIDGE:

                        for (int i = 0; i < docker.getPortMappingsCount(); i++) {
                            final PortMapping portMapping = docker.getPortMappings(i);
                            final int hostPort = portMapping.getHostPort();

                            if (hostPort > 0) {
                                // If the app is requesting a specific host port, then it needs to be available in the Offer.
                                if (!availablePortsQueue.contains(hostPort)) {
                                    return false;
                                }

                            } // If hostPort is 0, then we are allocating one from the available ports.

                        }

                        return availablePortsQueue.size() >= docker.getPortMappingsCount();

                    case HOST:
                        final Protos.Ports ports = aTaskInfo.getDiscovery().getPorts();

                        for (int i = 0; i < ports.getPortsCount(); i++) {
                            final Protos.Port port = ports.getPorts(i);

                            if (port.getNumber() > 0) {

                                if (!availablePortsQueue.contains(port.getNumber())) {
                                    return false;
                                }

                            }

                        }

                        return availablePortsQueue.size() >= aTaskInfo.getDiscovery().getPorts().getPortsCount();

                }

            default:
                return true;

        }

    }

    public synchronized void add(Protos.TaskInfo.Builder aTaskInfo) {
        mAllocatedCpus += MesosUtils.getCpus(aTaskInfo, 0);
        mAllocatedMem += MesosUtils.getMem(aTaskInfo, 0);
        mAllocatedDisk += MesosUtils.getDisk(aTaskInfo, 0);
        mAllocatedGpus += MesosUtils.getGpus(aTaskInfo, 0);

        allocatePorts(aTaskInfo);

        mAllocatedTasks.add(aTaskInfo);
    }

    private void allocatePorts(Protos.TaskInfo.Builder aTaskInfo) {
        final Protos.ContainerInfo.Builder container = aTaskInfo.getContainerBuilder();

        final List<Integer> allocatedPorts = new ArrayList<>();

        if (container.getType() == Protos.ContainerInfo.Type.DOCKER) {
            final Protos.ContainerInfo.DockerInfo.Builder dockerInfo = container.getDockerBuilder();

            allocatedPorts.addAll(addDefinedPorts(aTaskInfo, dockerInfo));
            allocatedPorts.addAll(addDynamicPorts(aTaskInfo, dockerInfo));
        }

        allocatedPorts.sort(Comparator.naturalOrder());

        final Protos.Value.Ranges.Builder ranges = Protos.Value.Ranges.newBuilder();

        if (allocatedPorts.size() > 0) {
            Protos.Value.Range.Builder range = Protos.Value.Range.newBuilder();

            long lastPort = allocatedPorts.get(0);

            range.setBegin(lastPort);

            for (int i = 1; i < allocatedPorts.size(); i++) {
                final long port = allocatedPorts.get(i);

                if (port > lastPort + 1) {
                    range.setEnd(lastPort);
                    ranges.addRange(range);

                    range = Protos.Value.Range.newBuilder();
                    range.setBegin(port);
                }

                lastPort = port;
            }

            range.setEnd(lastPort);
            ranges.addRange(range);

            aTaskInfo.addResources(MesosUtils.createPortsResource(ranges.build()));
        }

    }

    private List<Integer> addDefinedPorts(Protos.TaskInfo.Builder aTaskInfo, Protos.ContainerInfo.DockerInfo.Builder dockerInfo) {
        final List<Integer> allocatedPorts = new ArrayList<>();

        switch (dockerInfo.getNetwork()) {

            case BRIDGE:
                for (int i = 0; i < dockerInfo.getPortMappingsCount(); i++) {
                    final PortMapping portMapping = dockerInfo.getPortMappings(i);
                    final int hostPort = portMapping.getHostPort();

                    if (hostPort > 0) {
                        availablePortsQueue.remove(hostPort);
                        allocatedPorts.add(hostPort);
                    }

                }
                break;

            case HOST:
                final Protos.Ports ports = aTaskInfo.getDiscovery().getPorts();

                for (int i = 0; i < ports.getPortsCount(); i++) {
                    final Protos.Port port = ports.getPorts(i);
                    final int portNumber = port.getNumber();

                    if (portNumber > 0) {
                        availablePortsQueue.remove(portNumber);
                        allocatedPorts.add(portNumber);
                    }

                }

                break;

        }

        return allocatedPorts;
    }

    private List<Integer> addDynamicPorts(Protos.TaskInfo.Builder aTaskInfo, Protos.ContainerInfo.DockerInfo.Builder dockerInfo) {
        final List<Integer> allocatedPorts = new ArrayList<>();

        switch (dockerInfo.getNetwork()) {

            case BRIDGE:

                for (int i = 0; i < dockerInfo.getPortMappingsCount(); i++) {
                    final PortMapping.Builder portMapping = dockerInfo.getPortMappingsBuilder(i);
                    final int hostPort = portMapping.getHostPort();

                    if (hostPort == 0) {
                        final int port = availablePortsQueue.remove();

                        portMapping.setHostPort(port);
                        allocatedPorts.add(port);
                    }

                }

                break;

            case HOST:
                final Protos.Ports.Builder ports = aTaskInfo.getDiscoveryBuilder().getPortsBuilder();

                for (int i = 0; i < ports.getPortsCount(); i++) {
                    final Protos.Port.Builder port = ports.getPortsBuilder(i);
                    final int portNumber = port.getNumber();

                    if (portNumber == 0) {
                        final int nextPort = availablePortsQueue.remove();

                        port.setNumber(nextPort);
                        allocatedPorts.add(nextPort);
                    }

                }

                break;

        }

        return allocatedPorts;
    }

    public double getOfferCpus() {
        return mOfferCpus;
    }

    public double getOfferMem() {
        return mOfferMem;
    }

    public double getOfferDisk() {
        return mOfferDisk;
    }

    public double getOfferGpus() {
        return mOfferGpus;
    }

    public double getAllocatedCpus() {
        return mAllocatedCpus;
    }

    public double getAllocatedMem() {
        return mAllocatedMem;
    }

    public double getAllocatedDisk() {
        return mAllocatedDisk;
    }

    public double getAllocatedGpus() {
        return mAllocatedGpus;
    }

    public List<Protos.TaskInfo.Builder> getAllocatedTasks() {
        return mAllocatedTasks;
    }

    public Protos.OfferID getOfferID() {
        return mOfferID;
    }
}

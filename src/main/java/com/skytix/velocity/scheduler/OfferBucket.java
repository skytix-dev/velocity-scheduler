package com.skytix.velocity.scheduler;

import com.skytix.velocity.entities.TaskDefinition;
import com.skytix.velocity.mesos.MesosUtils;
import org.apache.mesos.v1.Protos;

import java.util.ArrayList;
import java.util.List;

public class OfferBucket {
    private final Protos.OfferID mOfferID;
    private final List<Protos.TaskInfo> mAllocatedTasks = new ArrayList<>();

    private final double mOfferCpus;
    private final double mOfferMem;
    private final double mOfferDisk;
    private final double mOfferGpus;

    private double mAllocatedCpus = 0.0;
    private double mAllocatedMem = 0.0;
    private double mAllocatedDisk = 0.0;
    private double mAllocatedGpus = 0.0;

    public OfferBucket(Protos.Offer aOffer) {
        mOfferID = aOffer.getId();
        mOfferCpus = MesosUtils.getNamedResourceScalar("cpus", aOffer, 0);
        mOfferMem = MesosUtils.getNamedResourceScalar("mem", aOffer, 0);
        mOfferDisk = MesosUtils.getNamedResourceScalar("disk", aOffer, 0);
        mOfferGpus = MesosUtils.getNamedResourceScalar("gpus", aOffer, 0);
    }

    public synchronized boolean hasResources(TaskDefinition aTaskDefinition) {

        return mAllocatedCpus + aTaskDefinition.getCpus() < mOfferCpus &&
                mAllocatedMem + aTaskDefinition.getMem() < mOfferMem &&
                mAllocatedDisk + aTaskDefinition.getDisk() < mOfferDisk &&
                mAllocatedGpus + aTaskDefinition.getGpus() < mOfferGpus;
    }

    public synchronized void add(Protos.TaskInfo aTaskInfo) {
        mAllocatedCpus += MesosUtils.getNamedResourceScalar("cpus", aTaskInfo, 0);
        mAllocatedMem += MesosUtils.getNamedResourceScalar("mem", aTaskInfo, 0);
        mAllocatedDisk += MesosUtils.getNamedResourceScalar("disk", aTaskInfo, 0);
        mAllocatedGpus += MesosUtils.getNamedResourceScalar("gpus", aTaskInfo, 0);

        mAllocatedTasks.add(aTaskInfo);
    }

    public List<Protos.TaskInfo> getAllocatedTasks() {
        return mAllocatedTasks;
    }

    public Protos.OfferID getOfferID() {
        return mOfferID;
    }
}

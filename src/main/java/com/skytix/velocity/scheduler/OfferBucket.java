package com.skytix.velocity.scheduler;

import com.skytix.velocity.mesos.MesosUtils;
import org.apache.mesos.v1.Protos;

import java.util.ArrayList;
import java.util.List;

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
    }

    public synchronized boolean hasResources(Protos.TaskInfoOrBuilder aTaskInfo) throws OfferBucketFullException {

        if (mAllocatedTasks.size() < mMaxTasksPerOffer) {

            if (mOfferCpus > 0 && mOfferMem > 0) {
                return mAllocatedCpus + MesosUtils.getCpus(aTaskInfo, 0) <= mOfferCpus &&
                        mAllocatedMem + MesosUtils.getMem(aTaskInfo, 0) <= mOfferMem &&
                        mAllocatedDisk + MesosUtils.getDisk(aTaskInfo, 0) <= mOfferDisk &&
                        mAllocatedGpus + MesosUtils.getGpus(aTaskInfo, 0) <= mOfferGpus;

            } else {
                // If there's no cpu or mem available.  Then nothing more will run.
                throw new OfferBucketFullException();
            }

        } else {
            throw new OfferBucketFullException();
        }

    }

    public synchronized void add(Protos.TaskInfo.Builder aTaskInfo) {
        mAllocatedCpus += MesosUtils.getCpus(aTaskInfo, 0);
        mAllocatedMem += MesosUtils.getMem(aTaskInfo, 0);
        mAllocatedDisk += MesosUtils.getDisk(aTaskInfo, 0);
        mAllocatedGpus += MesosUtils.getGpus(aTaskInfo, 0);

        mAllocatedTasks.add(aTaskInfo);
    }

    public List<Protos.TaskInfo.Builder> getAllocatedTasks() {
        return mAllocatedTasks;
    }

    public Protos.OfferID getOfferID() {
        return mOfferID;
    }
}

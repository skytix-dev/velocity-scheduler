package com.skytix.velocity.entities;

public class TaskDefinition {
    private final double mCpus;
    private final double mMem;
    private final double mDisk;
    private final double mGpus;

    public TaskDefinition(double aCpus, double aMem, double aDisk, double aGpus) {
        mCpus = aCpus;
        mMem = aMem;
        mDisk = aDisk;
        mGpus = aGpus;
    }

    public double getCpus() {
        return mCpus;
    }

    public double getMem() {
        return mMem;
    }

    public double getDisk() {
        return mDisk;
    }

    public double getGpus() {
        return mGpus;
    }

}

package com.skytix.velocity.scheduler;

import com.skytix.schedulerclient.SchedulerRemote;

public interface SchedulerRemoteProvider {
    public SchedulerRemote get();
}

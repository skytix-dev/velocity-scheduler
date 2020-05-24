package com.skytix.velocity.scheduler;

import lombok.Builder;
import lombok.Getter;
import org.apache.mesos.v1.Protos;

@Builder
@Getter
public class VelocityTaskRemote implements TaskRemote {
    private SchedulerRemoteProvider schedulerRemote;
    private Protos.AgentID agentID;
    private Protos.TaskID taskID;

    @Override
    public void terminate() {
        schedulerRemote.get().kill(taskID, agentID);
    }

}

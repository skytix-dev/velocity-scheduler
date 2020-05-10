package com.skytix.velocity.scheduler;

import com.skytix.schedulerclient.SchedulerRemote;
import lombok.Builder;
import lombok.Getter;
import org.apache.mesos.v1.Protos;

@Builder
@Getter
public class VelocityTaskRemote implements TaskRemote {
    private SchedulerRemote schedulerRemote;
    private Protos.AgentID agentID;
    private Protos.TaskID taskID;

    @Override
    public void terminate() {
        schedulerRemote.kill(taskID, agentID);
    }

}

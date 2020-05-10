package com.skytix.velocity.entities;

import com.skytix.velocity.scheduler.TaskRemote;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.mesos.v1.Protos;

import java.time.LocalDateTime;

@Getter
@Setter
@Builder
public class VelocityTask implements Task {
    private TaskDefinition taskDefinition;
    private Protos.TaskInfo taskInfo;
    private TaskRemote remote;
    private LocalDateTime created;
    private LocalDateTime startTime;
    private Protos.TaskState state;
    @Builder.Default
    private boolean started = false;
    @Builder.Default
    private int taskRetries = 0;

    public void incrementRetry() {
        taskRetries++;
    }

    @Override
    public int compareTo(VelocityTask o) {
        return new CompareToBuilder()
                .append(this.created, o.created)
                .append(this.taskDefinition.getTaskInfo().getTaskId().getValue(), o.taskDefinition.getTaskInfo().getTaskId().getValue())
                .toComparison();
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (obj.getClass() != getClass()) {
            return false;
        }

        final VelocityTask rhs = (VelocityTask) obj;

        return new EqualsBuilder()
                .append(this.taskDefinition.getTaskInfo().getTaskId().getValue(), rhs.taskDefinition.getTaskInfo().getTaskId().getValue())
                .build();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(223, 555)
                .append(taskDefinition.getTaskInfo().getTaskId().getValue())
                .toHashCode();
    }

}

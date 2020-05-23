package com.skytix.velocity.entities;

import com.skytix.velocity.scheduler.OfferPredicate;
import com.skytix.velocity.scheduler.TaskEventHandler;
import lombok.Builder;
import lombok.Getter;
import org.apache.mesos.v1.Protos;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Getter
@Builder
public class TaskDefinition {
    private final Protos.TaskInfo.Builder taskInfo;
    private final TaskEventHandler taskEventHandler;
    private final List<OfferPredicate> conditions;

    public static TaskDefinition from(Protos.TaskInfo.Builder aTaskInfo, OfferPredicate... aConditions) {
        return from(aTaskInfo, null, aConditions);
    }

    public static TaskDefinition from(Protos.TaskInfo.Builder aTaskInfo, TaskEventHandler aEventHandler, OfferPredicate... aConditions) {

        return TaskDefinition.builder()
                .taskInfo(aTaskInfo)
                .taskEventHandler(aEventHandler)
                .conditions(aConditions != null ? Arrays.asList(aConditions) : Collections.emptyList())
                .build();
    }

    public boolean hasConditions() {
        return getConditions() != null && !getConditions().isEmpty();
    }

    public boolean hasTaskId() {
        return getTaskInfo() != null && getTaskInfo().getTaskId() != null;
    }

}

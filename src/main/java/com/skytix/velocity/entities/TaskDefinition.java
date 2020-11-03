package com.skytix.velocity.entities;

import com.skytix.velocity.scheduler.OfferPredicate;
import com.skytix.velocity.scheduler.Priority;
import com.skytix.velocity.scheduler.TaskEventHandler;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.v1.Protos;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Getter
@Builder
public class TaskDefinition {
    private final Protos.TaskInfo.Builder taskInfo;
    private final Enum<? extends Priority> priority;
    private final boolean yieldToHigherPriority; // Flag that this task should wait till any higher priority tasks are completed
    private final TaskEventHandler taskEventHandler;
    private final List<OfferPredicate> conditions;
    private final double memoryTolerance = 0.0; // Default memory tolerance set to 0%.  Task will launch if an offer has within x% of the demanded memory.

    public static TaskDefinition from(Protos.TaskInfo.Builder aTaskInfo, Enum<? extends Priority> aPriority, OfferPredicate... aConditions) {
        return from(aTaskInfo, aPriority, null, aConditions);
    }

    public static TaskDefinition from(Protos.TaskInfo.Builder aTaskInfo, Enum<? extends Priority> aPriority, TaskEventHandler aEventHandler, OfferPredicate... aConditions) {
        return from(aTaskInfo, aPriority, false, aEventHandler, aConditions);
    }

    public static TaskDefinition from(Protos.TaskInfo.Builder aTaskInfo, Enum<? extends Priority> aPriority, boolean aYieldToHigherPriority, TaskEventHandler aEventHandler, OfferPredicate... aConditions) {

        return TaskDefinition.builder()
                .taskInfo(aTaskInfo)
                .priority(aPriority)
                .taskEventHandler(aEventHandler)
                .yieldToHigherPriority(aYieldToHigherPriority)
                .conditions(aConditions != null ? Arrays.asList(aConditions) : Collections.emptyList())
                .build();
    }

    public String getTaskId() {
        return hasTaskId() ? getTaskInfo().getTaskId().getValue() : null;
    }

    public boolean hasConditions() {
        return getConditions() != null && !getConditions().isEmpty();
    }

    public boolean hasTaskId() {
        return getTaskInfo() != null && StringUtils.isNotBlank(getTaskInfo().getTaskId().getValue());
    }

}

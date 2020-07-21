package com.skytix.velocity.scheduler;

import com.skytix.schedulerclient.SchedulerConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.util.concurrent.TimeUnit;

@SuperBuilder(toBuilder = true)
@Getter
public class VelocitySchedulerConfig extends SchedulerConfig {
    @Builder.Default
    private Integer maxOfferQueueSize = 1000;
    @Builder.Default
    private Integer maxUpdateQueueSize = 1000;
    @Builder.Default
    private Integer maxTaskQueueSize = 50000;
    @Builder.Default
    private boolean restrictedGpuScheduling = true;
    @Builder.Default
    private int taskRetryLimit = 3;
    @Builder.Default
    private int taskQueueFullWaitTimeout = 2;
    @Builder.Default
    private TimeUnit taskQueueFullWaitTimeoutUnit = TimeUnit.SECONDS;
    @Builder.Default
    private int heartbeatDelaySeconds = 2; // The maximum acceptable delay to receiving heartbeat messages from the master.
    @Builder.Default
    private TaskEventHandler defaultTaskEventHandler = null;
    @Builder.Default
    private HeartbeatListener heartbeatListener = null;
    @Builder.Default
    private Integer numOfferConsumers = 5;
    private Class<? extends Enum<? extends Priority>> priorites;
    private Priority defaultPriority;

}

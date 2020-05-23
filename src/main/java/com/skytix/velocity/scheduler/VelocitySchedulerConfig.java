package com.skytix.velocity.scheduler;

import com.skytix.schedulerclient.SchedulerConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.util.concurrent.TimeUnit;

@SuperBuilder
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
    private int taskQueueFullWaitTimeout = 2;
    @Builder.Default
    private TimeUnit taskQueueFullWaitTimeoutUnit = TimeUnit.SECONDS;

}

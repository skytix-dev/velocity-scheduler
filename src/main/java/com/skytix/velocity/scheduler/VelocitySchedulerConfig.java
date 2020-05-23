package com.skytix.velocity.scheduler;

import com.skytix.schedulerclient.SchedulerConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
public class VelocitySchedulerConfig extends SchedulerConfig {
    private Integer maxOfferQueueSize;
    private Integer maxUpdateQueueSize;
    @Builder.Default
    private boolean restrictedGpuScheduling = true;
}

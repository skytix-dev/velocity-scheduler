package com.skytix.velocity.scheduler;

import com.skytix.schedulerclient.BaseSchedulerEventHandler;
import com.skytix.velocity.services.SchedulerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.scheduler.Protos;

import java.io.IOException;

@Slf4j
public class DrainingSchedulerHandler extends BaseSchedulerEventHandler {
    private final SchedulerService mSchedulerService;

    private long numActiveTasks = 0;

    public DrainingSchedulerHandler(SchedulerService aSchedulerService) {
        mSchedulerService = aSchedulerService;
    }

    @Override
    public void onSubscribe() {
        final String frameworkId = getSchedulerRemote().getFrameworkID().getValue();
        numActiveTasks = mSchedulerService.getNumActiveTasks(frameworkId);

        if (numActiveTasks == 0) {

            try {
                getSchedulerRemote().teardown();

            } catch (IOException aE) {
                log.error(aE.getMessage(), aE);
            }

        }

    }

    @Override
    public void handleEvent(Protos.Event aEvent) throws Exception {

    }

}

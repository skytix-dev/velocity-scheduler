package com.skytix.velocity.scheduler;

import com.skytix.schedulerclient.BaseSchedulerEventHandler;
import com.skytix.velocity.services.SchedulerService;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.apache.mesos.v1.scheduler.Protos.Event.Offers;

public class StandardSchedulerHandler extends BaseSchedulerEventHandler {
    private final SchedulerService mSchedulerService;

    public StandardSchedulerHandler(SchedulerService aSchedulerService) {
        mSchedulerService = aSchedulerService;
    }

    @Override
    public void handleEvent(Event aEvent) throws Exception {

        switch (aEvent.getType()) {

            case OFFERS:
                final Offers offers = aEvent.getOffers();

                for (int i = 0; i < offers.getOffersCount(); i++) {
                    mSchedulerService.queueMesosOffer(offers.getOffers(i));
                }

                break;

            case RESCIND:
                handleRescind(aEvent.getRescind());
                break;

            case UPDATE:
                mSchedulerService.queueMesosUpdate(aEvent.getUpdate());
                break;

        }

    }

    private void handleRescind(Event.Rescind aRescind) {
        /* Since we either launch tasks or reject offers, there shouldn't be any offers on hold to rescind.  However if
         * we receive an offer and start finding tasks for it and a rescind request is received, we need to cancel it.
         */
    }

}

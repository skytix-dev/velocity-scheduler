package com.skytix.velocity.services;

import com.skytix.schedulerclient.Scheduler;
import com.skytix.schedulerclient.SchedulerRemote;
import com.skytix.velocity.entities.db.TaskDef;
import com.skytix.velocity.repository.DBRepository;
import com.skytix.velocity.scheduler.DrainingSchedulerHandler;
import com.skytix.velocity.scheduler.StandardSchedulerHandler;
import com.skytix.velocity.services.mesos.MesosService;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.Protos.Offer;
import org.apache.mesos.v1.master.Protos.Response.GetFrameworks.Framework;
import org.apache.mesos.v1.scheduler.Protos.Event.Update;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

@Component
@Slf4j
public class SchedulerService {
    private final BlockingQueue<Offer> mOfferQueue = new ArrayBlockingQueue<>(50); //TODO: Make this configurable.
    private final BlockingQueue<Update> mUpdateQueue = new LinkedBlockingQueue<>();
    private final String mFrameworkId = UUID.randomUUID().toString();
    private final Scheduler mMesosScheduler;
    private final MesosService mMesosService;
    private final StandardSchedulerHandler mSchedulerHandler;
    private final DBRepository mDBRepository;
    private final Future<?> mOfferQueueConsumer;
    private final Future<?> mUpdateQueueConsumer;
    private final Future<?> mInactiveFrameworkReconciler;
    private boolean mRunning = true;

    @Autowired
    public SchedulerService(DBRepository aDBRepository, MesosService aMesosService) {
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

        mSchedulerHandler = new StandardSchedulerHandler(this);
        mDBRepository = aDBRepository;
        mMesosService = aMesosService;
        mOfferQueueConsumer = executorService.submit(createOfferQueueConsumer());
        mUpdateQueueConsumer = executorService.submit(createUpdateQueueConsumer());

        mMesosScheduler = Scheduler.newScheduler(
                mFrameworkId,
                "http://10.9.10.1:5050",
                mSchedulerHandler
        );

        mDBRepository.registerScheduler(mFrameworkId);

        mInactiveFrameworkReconciler = executorService.scheduleAtFixedRate(createInactiveFrameworkReconciler(), 5, 15, TimeUnit.SECONDS);
    }

    public SchedulerRemote getSchedulerRemote() {
        return mSchedulerHandler.getSchedulerRemote();
    }

    public void queueTask(TaskDef aTaskDef) {
        mDBRepository.saveNewTask(aTaskDef);
    }

    public synchronized void queueMesosOffer(Offer aOffer) throws InterruptedException {

        if (!mOfferQueue.offer(aOffer, 2, TimeUnit.SECONDS)) {
            log.error(String.format("Timeout adding offer '%s' to queue.  Queue full.  Declining offer.", aOffer.getId().getValue()));
        }

    }

    public synchronized void queueMesosUpdate(Update aUpdate) throws InterruptedException {
        mUpdateQueue.put(aUpdate);
    }

    public long getNumQueuedTasks(String aFrameworkId) {
        return mDBRepository.getNumQueuedTasks(aFrameworkId);
    }

    public Long getNumActiveTasks(String aFrameworkId) {
        return mDBRepository.getNumActiveTasks(aFrameworkId);
    }

    private Runnable createUpdateQueueConsumer() {

        return () -> {

            try {

                while (mRunning) {
                    final Update update = mUpdateQueue.take();

                    // Do some stuff with the task.

                    mSchedulerHandler.getSchedulerRemote().acknowledge(update.getStatus());
                }

            } catch (InterruptedException aE) {
                log.info("UpdateQueueConsumer is exiting");
            }

        };

    }

    private Runnable createOfferQueueConsumer() {

        return () -> {

            try {

                while (mRunning) {
                    final Offer offer = mOfferQueue.take();

                    if (getNumQueuedTasks() > 0) {
                        // Search to find all the tasks that can fit in the offer
                        // 1. Find tasks that have attribute constraints that match the attributes on the Offer.

                        // 2. For remaining offer resources, find tasks that do not have any attribute constraints.

                    } else {
                        final org.apache.mesos.v1.Protos.OfferID offerId = offer.getId();
                        log.info(String.format("Declining offer: %s", offerId.getValue()));
                        mSchedulerHandler.getSchedulerRemote().decline(Collections.singletonList(offerId));
                    }

                }

            } catch (InterruptedException aE) {
                log.info("OfferQueueConsumer is exiting");
            }

        };

    }

    private Runnable createInactiveFrameworkReconciler() {

        return () -> {

            try {
                final List<Framework> inactiveFrameworks = mMesosService.getInactiveFrameworks();

                for (Framework framework : inactiveFrameworks) {

                    final Scheduler scheduler = Scheduler.newScheduler(
                            framework.getFrameworkInfo().getId().getValue(),
                            mMesosScheduler.getMesosMasterURL(),
                            new DrainingSchedulerHandler(this)
                    );

                    scheduler.join(); // This will wait till
                }

            } catch (Exception aE) {
                log.error(aE.getMessage(), aE);
            }

        };

    }

    @PreDestroy
    public void destroy() {

        try {
            mDBRepository.deregisterScheduler(mFrameworkId);
            mOfferQueueConsumer.cancel(true);
            mUpdateQueueConsumer.cancel(true);
            mInactiveFrameworkReconciler.cancel(true);
            mMesosScheduler.close();

        } catch (Exception aE) {
            log.error(aE.getMessage(), aE);
        }

    }

}

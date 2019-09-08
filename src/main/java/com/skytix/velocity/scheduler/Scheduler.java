package com.skytix.velocity.scheduler;

import com.skytix.velocity.entities.TaskDefinition;
import com.skytix.velocity.services.mesos.MesosService;
import lombok.extern.log4j.Log4j2;
import org.apache.mesos.v1.scheduler.Protos;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.mesos.v1.Protos.*;

@Component
@Log4j2
public class Scheduler implements DisposableBean {
    private static final int MAX_TASKS_PER_OFFER = 10;

    private final MesosService mMesosService;
    private FrameworkID mFrameworkId = null;
    private final TaskExecutor mTaskExecutor;
    private final HttpClient mHttpClient = HttpClient.newHttpClient();

    private BlockingQueue<Event> mEventQueue = new LinkedBlockingQueue<>(1000);
    private final Queue<TaskDefinition> mTaskQueue = new ConcurrentLinkedQueue<>();

    private String mMesosStreamID = null;
    private String mMesosMasterUrl = null;

    @Autowired
    public Scheduler(MesosService aMesosService, TaskExecutor aTaskExecutor) {
        mMesosService = aMesosService;
        mTaskExecutor = aTaskExecutor;
    }

    @PostConstruct
    public void init() {
        // Discover the Mesos leader from ZK.
        mMesosMasterUrl = "http://10.9.10.1:5050";

        // Start and connect the scheduler.
        initSchedulerClient(mMesosMasterUrl); //TODO: Make configurable
    }

    @Override
    @PreDestroy
    public void destroy() throws Exception {
        log.info("Closing down Scheduler");

        sendCall(
                Protos.Call.newBuilder()
                .setFrameworkId(mFrameworkId)
                .setType(Protos.Call.Type.TEARDOWN)
                .build()
        );
    }

    private void initSchedulerClient(String aMesosLeader) {
        final FrameworkID.Builder frameworkID = FrameworkID.newBuilder().setValue(UUID.randomUUID().toString());

        mTaskExecutor.execute(() -> {

            try {

                final FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
                        .setId(frameworkID)
                        .setUser("root")
                        .setName("Velocity Scheduler")
                        .setFailoverTimeout(10);

                final Protos.Call subscribeCall = Protos.Call.newBuilder()
                        .setFrameworkId(frameworkID)
                        .setType(Protos.Call.Type.SUBSCRIBE)
                        .setSubscribe(
                                Protos.Call.Subscribe.newBuilder().setFrameworkInfo(frameworkInfo)
                        )
                        .build();

                final HttpRequest request = HttpRequest.newBuilder()
                        .uri(new URI(aMesosLeader + "/api/v1/scheduler"))
                        .header("Content-Type", "application/x-protobuf")
                        .header("Accept", "application/x-protobuf")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(subscribeCall.toByteArray()))
                        .build();

                final HttpResponse<InputStream> response = mHttpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

                if (response.statusCode() == 200) {
                    mMesosStreamID = response.headers().firstValue("Mesos-Stream-Id").get();

                    InputStream reader = new BufferedInputStream(response.body());
                    StringBuffer sb = new StringBuffer();
                    int data = reader.read();

                    while (data != -1) {

                        if ( data == 10 ) {
                            // Contents of the StringBuffer should have the length of bytes to read.
                            final Long recordLength = Long.parseLong(sb.toString());
                            final byte[] buffer = new byte[recordLength.intValue()];

                            reader.read(buffer);

                            handleEvent(Event.parseFrom(buffer));

                            sb = new StringBuffer();
                            data = reader.read();

                        } else {
                            sb.append(new String(new byte[] {(byte)data}));
                            data = reader.read();
                        }

                    }

                } else {
                    log.error("Error subscribing to Mesos");
                }

            } catch (URISyntaxException | IOException | InterruptedException aE) {
                aE.printStackTrace();
            }

        });

        mFrameworkId = frameworkID.build();
    }

    private void handleEvent(Event aEvent) {

        switch (aEvent.getType()) {
            case ERROR:
            case FAILURE:
            case OFFERS:
                sendCall(handleOffer(aEvent));
                break;
            case UPDATE:

            case HEARTBEAT:
            case SUBSCRIBED:
                // Do Nothing for these messages for now
                break;
        }
    }

    private void sendCall(Protos.Call aCall) {

        try {
            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(mMesosMasterUrl + "/api/v1/scheduler"))
                    .header("Content-Type", "application/x-protobuf")
                    .header("Accept", "application/x-protobuf")
                    .header("Mesos-Stream-Id", mMesosStreamID)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(aCall.toByteArray()))
                    .build();

            final HttpResponse<String> response = mHttpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 202) {
                log.error("Error sending call to Mesos: " + response.body());
            }

        } catch (URISyntaxException | IOException | InterruptedException aE) {
            aE.printStackTrace();
        }

    }

    private Protos.Call handleOffer(Event aEvent) {
        //  All the fun begins here...
        final List<Offer> offers = aEvent.getOffers().getOffersList();
        final List<OfferBucket> offerBuckets = new ArrayList<>(offers.size());

        if (mTaskQueue.size() > 0) {
            final AtomicInteger tasksLaunched = new AtomicInteger(0);

            offers.forEach((offer) -> {
                final OfferBucket bucket = new OfferBucket(offer);

                if (tasksLaunched.get() < MAX_TASKS_PER_OFFER) {

                    for (TaskDefinition taskDef : mTaskQueue) {

                        if (bucket.hasResources(taskDef) && tasksLaunched.get() < MAX_TASKS_PER_OFFER) {
                            tasksLaunched.incrementAndGet();
                            bucket.add(buildTaskInfo(taskDef));
                        }

                    }

                }

                offerBuckets.add(bucket);
            });

            return buildAcceptCall(offerBuckets);

        } else {
            return buildRejectCall(offers);
        }

    }

    private Protos.Call buildAcceptCall(List<OfferBucket> aOfferBuckets) {

        final List<org.apache.mesos.v1.Protos.TaskInfo> taskInfos = aOfferBuckets.stream()
                .flatMap((bucket) -> bucket.getAllocatedTasks().stream())
                .collect(Collectors.toList());

        return Protos.Call.newBuilder()
                .setFrameworkId(mFrameworkId)
                .setType(Protos.Call.Type.ACCEPT)
                .setAccept(
                        Protos.Call.Accept.newBuilder()
                                .addAllOfferIds(aOfferBuckets.stream().map(OfferBucket::getOfferID).collect(Collectors.toList()))
                                .addOperations(
                                        Offer.Operation.newBuilder()
                                                .setType(Offer.Operation.Type.LAUNCH)
                                                .setLaunch(
                                                        Offer.Operation.Launch.newBuilder()
                                                                .addAllTaskInfos(taskInfos)
                                                )
                                )
                )
                .build();

    }

    private Protos.Call buildRejectCall(List<Offer> aOffers) {
        return Protos.Call.newBuilder()
                .setFrameworkId(mFrameworkId)
                .setType(Protos.Call.Type.DECLINE)
                .setDecline(
                        Protos.Call.Decline.newBuilder()
                        .addAllOfferIds(aOffers.stream().map(Offer::getId).collect(Collectors.toList()))
                ).build();
    }

    private TaskInfo buildTaskInfo(TaskDefinition aTaskDef) {
        return null;
    }

}

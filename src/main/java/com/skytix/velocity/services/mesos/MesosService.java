package com.skytix.velocity.services.mesos;

import com.skytix.velocity.scheduler.StandardSchedulerHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.v1.master.Protos;
import org.apache.mesos.v1.master.Protos.Response.GetFrameworks.Framework;
import org.apache.mesos.v1.master.Protos.Response.GetState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
@Slf4j
public class MesosService {
    private final HttpClient mHttpClient = HttpClient.newHttpClient();

    @Autowired
    public MesosService() {
    }

    @Lookup
    public StandardSchedulerHandler getSchedulerHandler() {
        return null;
    }

    public List<Framework> getInactiveFrameworks() {

        if (getSchedulerHandler().getSchedulerRemote() != null) {

            try {
                final List<Framework> inactiveFrameworks = new ArrayList<>();
                final String leader = getSchedulerHandler().getSchedulerRemote().getMesosMasterURL();
                final URI leaderUri = new URI(String.format("%s/api/v1", leader));

                final HttpRequest request = HttpRequest.newBuilder()
                        .uri(leaderUri)
                        .header("Content-Type", "application/x-protobuf")
                        .header("Accept", "application/x-protobuf")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(Protos.Call.newBuilder().setType(Protos.Call.Type.GET_STATE).build().toByteArray()))
                        .build();

                final HttpResponse<byte[]> response = mHttpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

                if (response.statusCode() == 200) {
                    final GetState state = Protos.Response.parseFrom(response.body()).getGetState();

                    for (Framework framework : state.getGetFrameworks().getFrameworksList()) {

                        if (!framework.getActive() && !framework.getConnected()) {
                            inactiveFrameworks.add(framework);
                        }

                    }

                } else {
                    log.error(String.format("Unable to get status from Mesos leader: %d", response.statusCode()));
                }

                return inactiveFrameworks;

            } catch (URISyntaxException | IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }

        } else {
            log.warn("Scheduler not connected. Unable to detect inactive frameworks.");
            return Collections.emptyList();
        }

    }


}

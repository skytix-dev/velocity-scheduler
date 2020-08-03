import com.skytix.velocity.VelocityMesosScheduler;
import com.skytix.velocity.entities.TaskDefinition;
import com.skytix.velocity.mesos.Tasks;
import com.skytix.velocity.scheduler.DefaultPriority;
import com.skytix.velocity.scheduler.VelocitySchedulerConfig;
import org.apache.mesos.v1.Protos;

import java.util.concurrent.atomic.AtomicInteger;

public class TestApp {

    public static void main(String[] args) {

        try {
            final VelocitySchedulerConfig config = VelocitySchedulerConfig.builder()
                    .frameworkID("marc-test-scheduler-1")
                    .mesosMasterURL("http://10.9.10.1:5050")
                    .priorites(DefaultPriority.class)
                    .disableSSLTrust(true)
                    .enableGPUResources(true)
                    .restrictedGpuScheduling(false)
                    .build();

            final VelocityMesosScheduler scheduler = new VelocityMesosScheduler(config);
            int idx = 0;

            for (int i = 0; i < 100; i++) {
                final AtomicInteger atomicInteger = new AtomicInteger(idx);

                final Protos.TaskInfo.Builder docker = Tasks.docker("My special test", "ubuntu", 0.1, 0, 1000, 0, true, "ls -la; sleep 10");

                final TaskDefinition taskDef = TaskDefinition.from(
                        docker,
                        DefaultPriority.STANDARD,
                        (event) -> {

                            switch (event.getStatus().getState()) {
                                case TASK_FINISHED:
                                    System.out.println(String.format("Job number %d finished", atomicInteger.get()));
                            }

                        }
                );

                scheduler.launch(taskDef);
                System.out.println("Queued job " + idx);
                idx++;
            }

            scheduler.drainAndClose();

        } catch (Exception aE) {
            aE.printStackTrace();
        }
    }

}

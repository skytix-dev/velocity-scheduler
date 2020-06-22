import com.skytix.velocity.VelocityMesosScheduler;
import com.skytix.velocity.entities.TaskDefinition;
import com.skytix.velocity.mesos.Tasks;
import com.skytix.velocity.scheduler.MesosScheduler;
import com.skytix.velocity.scheduler.VelocitySchedulerConfig;

import java.util.concurrent.atomic.AtomicInteger;

public class TestApp {

    public static void main(String[] args) {

        try {
            final VelocitySchedulerConfig config = VelocitySchedulerConfig.builder()
                    .frameworkID("marc-test-scheduler")
                    .mesosMasterURL("http://10.9.10.1:5050")
                    .disableSSLTrust(true)
                    .enableGPUResources(true)
                    .restrictedGpuScheduling(false)
                    .build();

            final MesosScheduler scheduler = new VelocityMesosScheduler(config);
            int idx = 0;

            while (true) {

                for (int i = 0; i < 2; i++) {
                    final AtomicInteger atomicInteger = new AtomicInteger(idx);

                    final TaskDefinition taskDef = TaskDefinition.from(
                            Tasks.docker("My special test", "ubuntu", "ls -la", 0.1, 0, 0.01, 0),
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

                Thread.sleep(1000);

            }

        } catch (Exception aE) {
            aE.printStackTrace();
        }
    }

}

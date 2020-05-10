import com.skytix.schedulerclient.SchedulerConfig;
import com.skytix.velocity.VelocityMesosScheduler;
import com.skytix.velocity.entities.TaskDefinition;
import com.skytix.velocity.mesos.Tasks;
import com.skytix.velocity.scheduler.MesosScheduler;

public class TestApp {

    public static void main(String[] args) {

        try {
            final SchedulerConfig config = SchedulerConfig.builder()
                    .frameworkID("marc-test-scheduler")
                    .mesosMasterURL("http://10.9.10.1:5050")
                    .disableSSLTrust(true)
                    .enableGPUResources(true)
                    .build();

            final MesosScheduler scheduler = new VelocityMesosScheduler(config);

            for (int i = 0; i < 100; i++) {
                final int idx = i;

                final TaskDefinition taskDef = TaskDefinition.from(
                        Tasks.docker("My special test", "ubuntu", "ls -la", 0.1, 0, 0.01, 0),
                        (event) -> {

                            switch (event.getStatus().getState()) {
                                case TASK_FINISHED:
                                    System.out.println(String.format("Job number %d finished", idx));
                            }
                        }
                );

                scheduler.launch(taskDef);
            }

            while (true) {
                Thread.sleep(1000);
            }

        } catch (Exception aE) {
            aE.printStackTrace();
        }
    }

}

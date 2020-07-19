package com.skytix.velocity.mesos;

import com.skytix.schedulerclient.mesos.MesosConstants;
import org.apache.mesos.v1.Protos;

import java.util.Arrays;
import java.util.UUID;

public final class Tasks {

    public static Protos.TaskInfo.Builder docker(String taskName, String image, double cpu, double gpu, double mem, double disk, boolean shell, String command, String... args) {

        final Protos.CommandInfo.Builder commandInfo = Protos.CommandInfo.newBuilder()
                .setValue(command)
                .setShell(shell);

        if (args != null && args.length > 0) {
            commandInfo.addAllArguments(Arrays.asList(args));
        }

        final Protos.TaskInfo.Builder taskInfo = task(taskName, cpu, gpu, mem, disk)
                .setContainer(
                        Protos.ContainerInfo.newBuilder()
                                .setType(Protos.ContainerInfo.Type.DOCKER)
                                .setDocker(
                                        Protos.ContainerInfo.DockerInfo.newBuilder()
                                                .setImage(image)
                                                .setPrivileged(false)
                                )

                )
                .setCommand(commandInfo);

        return taskInfo;
    }

    public static Protos.TaskInfo.Builder task(String taskName, double cpu, double gpu, double mem, double disk) {
        final Protos.TaskInfo.Builder taskInfo = Protos.TaskInfo.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setName(taskName);

        taskInfo.addResources(
                Protos.Resource.newBuilder()
                        .setName(MesosConstants.SCALAR_CPU)
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpu))
                        .build()
        );

        taskInfo.addResources(
                Protos.Resource.newBuilder()
                        .setName(MesosConstants.SCALAR_MEM)
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem))
                        .build()
        );

        if (gpu > 0) {
            taskInfo.addResources(
                    Protos.Resource.newBuilder()
                            .setName(MesosConstants.SCALAR_GPU)
                            .setType(Protos.Value.Type.SCALAR)
                            .setScalar(Protos.Value.Scalar.newBuilder().setValue(gpu))
                            .build()
            );

        }

        if (disk > 0) {
            taskInfo.addResources(
                    Protos.Resource.newBuilder()
                            .setName(MesosConstants.SCALAR_DISK)
                            .setType(Protos.Value.Type.SCALAR)
                            .setScalar(Protos.Value.Scalar.newBuilder().setValue(disk))
                            .build()
            );

        }

        return taskInfo;
    }

    private Tasks() {

    }

}

package com.skytix.velocity.mesos;

import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo.Network;

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
                                                .setNetwork(Network.BRIDGE)
                                )

                )
                .setCommand(commandInfo);

        return taskInfo;
    }

    public static Protos.TaskInfo.Builder task(String taskName, double cpu, double gpu, double mem, double disk) {
        final Protos.TaskInfo.Builder taskInfo = Protos.TaskInfo.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setName(taskName);

        taskInfo.addResources(MesosUtils.createCpuResource(cpu));
        taskInfo.addResources(MesosUtils.createMemResource(mem));

        if (gpu > 0) {
            taskInfo.addResources(MesosUtils.createGpuResource(gpu));
        }

        if (disk > 0) {
            taskInfo.addResources(MesosUtils.createDiskResource(disk));
        }

        return taskInfo;
    }

    private Tasks() {

    }

}

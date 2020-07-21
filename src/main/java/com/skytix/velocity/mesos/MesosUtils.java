package com.skytix.velocity.mesos;

import com.skytix.schedulerclient.mesos.MesosConstants;
import com.skytix.velocity.entities.VelocityTask;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.scheduler.Protos.Call.Reconcile.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;

public class MesosUtils {

    public static Protos.Resource getNamedResource(String aName, Protos.TaskInfoOrBuilder aTaskInfo) {
        return getResource(aName, aTaskInfo.getResourcesList());
    }

    public static Protos.Resource getNamedResource(String aName, Protos.OfferOrBuilder aOffer) {
        return getResource(aName, aOffer.getResourcesList());
    }

    private static Protos.Resource getResource(String aName, List<Protos.Resource> aResourcesList) {
        final Map<String, List<Protos.Resource>> resources = aResourcesList
                .stream()
                .collect(groupingBy(Protos.Resource::getName));

        if (resources != null && resources.containsKey(aName)) {
            return resources.get(aName).get(0);

        } else {
            return null;
        }
    }

    public static double getCpus(Protos.OfferOrBuilder aOffer, double aDefaultValue) {
        return getNamedResourceScalar(MesosConstants.SCALAR_CPU, aOffer, aDefaultValue);
    }

    public static double getCpus(Protos.TaskInfoOrBuilder aTaskInfo, double aDefaultValue) {
        return getNamedResourceScalar(MesosConstants.SCALAR_CPU, aTaskInfo, aDefaultValue);
    }

    public static Protos.Resource createCpuResource(double aCpus) {
        return createNamedScalarResource(MesosConstants.SCALAR_CPU, aCpus);
    }

    public static double getGpus(Protos.OfferOrBuilder aOffer, double aDefaultValue) {
        return getNamedResourceScalar(MesosConstants.SCALAR_GPU, aOffer, aDefaultValue);
    }

    public static double getGpus(Protos.TaskInfoOrBuilder aTaskInfo, double aDefaultValue) {
        return getNamedResourceScalar(MesosConstants.SCALAR_GPU, aTaskInfo, aDefaultValue);
    }

    public static Protos.Resource createGpuResource(double aGpus) {
        return createNamedScalarResource(MesosConstants.SCALAR_GPU, aGpus);
    }

    public static double getMem(Protos.OfferOrBuilder aOffer, double aDefaultValue) {
        return getNamedResourceScalar(MesosConstants.SCALAR_MEM, aOffer, aDefaultValue);
    }

    public static double getMem(Protos.TaskInfoOrBuilder aTaskInfo, double aDefaultValue) {
        return getNamedResourceScalar(MesosConstants.SCALAR_MEM, aTaskInfo, aDefaultValue);
    }

    public static Protos.Resource createMemResource(double aMem) {
        return createNamedScalarResource(MesosConstants.SCALAR_MEM, aMem);
    }

    public static double getDisk(Protos.OfferOrBuilder aOffer, double aDefaultValue) {
        return getNamedResourceScalar(MesosConstants.SCALAR_DISK, aOffer, aDefaultValue);
    }

    public static double getDisk(Protos.TaskInfoOrBuilder aTaskInfo, double aDefaultValue) {
        return getNamedResourceScalar(MesosConstants.SCALAR_DISK, aTaskInfo, aDefaultValue);
    }

    public static Protos.Resource createDiskResource(double aDisk) {
        return createNamedScalarResource(MesosConstants.SCALAR_DISK, aDisk);
    }

    public static Protos.Resource createNamedScalarResource(String aName, double aValue) {

        return Protos.Resource.newBuilder()
                .setName(aName)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(aValue))
                .build();
    }

    public static double getNamedResourceScalar(String aName, Protos.OfferOrBuilder aOffer, double aDefaultValue) {
        final Protos.Resource resource = getNamedResource(aName, aOffer);

        if (resource != null) {
            return resource.getScalar().getValue();

        } else {
            return aDefaultValue;
        }

    }

    public static double getNamedResourceScalar(String aName, Protos.TaskInfoOrBuilder aTaskInfo, double aDefaultValue) {
        final Protos.Resource resource = getNamedResource(aName, aTaskInfo);

        if (resource != null) {
            return resource.getScalar().getValue();

        } else {
            return aDefaultValue;
        }
    }

    public static List<Task> buildReconcileTasks(List<VelocityTask> aActiveTasks) {
        final List<Task> results = new ArrayList<>(aActiveTasks.size());

        aActiveTasks.forEach((aVelocityTask -> {
            results.add(
                    Task.newBuilder()
                            .setTaskId(aVelocityTask.getTaskInfo().getTaskId())
                            .setAgentId(aVelocityTask.getTaskInfo().getAgentId())
                            .build()
            );

        }));

        return results;
    }

}

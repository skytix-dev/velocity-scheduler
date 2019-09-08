package com.skytix.velocity.mesos;

import org.apache.mesos.v1.Protos;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;

public class MesosUtils {

    public static Protos.Resource getNamedResource(String aName, Protos.TaskInfo aTaskInfo) {
        return getResource(aName, aTaskInfo.getResourcesList());
    }

    public static Protos.Resource getNamedResource(String aName, Protos.Offer aOffer) {
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

    public static double getNamedResourceScalar(String aName, Protos.Offer aOffer, double aDefaultValue) {
        final Protos.Resource resource = getNamedResource(aName, aOffer);

        if (resource != null) {
            return resource.getScalar().getValue();

        } else {
            return aDefaultValue;
        }

    }

    public static double getNamedResourceScalar(String aName, Protos.TaskInfo aTaskInfo, double aDefaultValue) {
        final Protos.Resource resource = getNamedResource(aName, aTaskInfo);

        if (resource != null) {
            return resource.getScalar().getValue();

        } else {
            return aDefaultValue;
        }
    }

}

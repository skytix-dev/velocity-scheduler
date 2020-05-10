package com.skytix.velocity.scheduler;

import org.apache.mesos.v1.Protos;

import java.util.List;

public final class OfferPredicates {

    public static OfferPredicate pAttrMatches(String aAttributeName, double aValue) {

        return (offer) -> {
            final List<Protos.Attribute> attributesList = offer.getAttributesList();

            for (Protos.Attribute attribute : attributesList) {

                if (attribute.getName().equals(aAttributeName)) {
                    return attribute.getScalar().getValue() == aValue;
                }

            }

            return false;
        };

    }

    public static OfferPredicate pAttrMatches(String aAttributeName, String aValue) {

        return (offer) -> {
            final List<Protos.Attribute> attributesList = offer.getAttributesList();

            for (Protos.Attribute attribute : attributesList) {

                if (attribute.getName().equals(aAttributeName)) {
                    return attribute.getText().getValue().equals(aValue);
                }

            }

            return false;
        };

    }

}

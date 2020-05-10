package com.skytix.velocity.scheduler;

import org.apache.mesos.v1.Protos;

public interface OfferPredicate {
    public boolean test(Protos.Offer aOffer);

}

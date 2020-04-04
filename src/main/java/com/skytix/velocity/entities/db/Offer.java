package com.skytix.velocity.entities.db;

import lombok.AccessLevel;
import lombok.Builder;

import javax.persistence.Table;

@Table(name = "Offers")
@Builder(access = AccessLevel.PUBLIC)
public class Offer extends CrateDBObject {
    public String offer_id;
    public String framework_id;
    public String agent_id;
    public String hostname;
    public double cpus;
    public double mem;
    public double disk;
    public double gpus;
    public String[] attributes;

}

package com.skytix.velocity.entities.db;

import javax.persistence.Table;
import java.time.ZonedDateTime;

@Table(name = "Schedulers")
public class SchedulerFramework extends CrateDBObject {
    public String framework_id;
    public Boolean recovering;
    public ZonedDateTime registered;
    public ZonedDateTime recovered_time;
}

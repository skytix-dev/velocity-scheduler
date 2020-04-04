package com.skytix.velocity.entities.db;

import javax.persistence.Table;
import java.time.ZonedDateTime;

@Table(name="tasks")
public class TaskDef extends CrateDBObject {
    public String task_id;
    public String framework_id;
    public ZonedDateTime task_created;
    public String name;
    public String command;
    public String image;
    public boolean forcepull;
    public double cpus;
    public double mem;
    public double disk;
    public double gpus;
}

package com.skytix.velocity.repository;

import com.dieselpoint.norm.Database;
import com.skytix.velocity.entities.db.SchedulerFramework;
import com.skytix.velocity.entities.db.TaskDef;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.util.List;

@Component
public class DBRepository {
    @Autowired
    private Database mDatabase;

    public TaskDef getTaskByTaskId(String aTaskId) {
        final List<TaskDef> tasks = mDatabase.where("task_id", aTaskId).results(TaskDef.class);

        if (tasks != null && tasks.size() > 0) {
            return tasks.get(0);

        } else {
            return null;
        }

    }

    public void saveNewTask(TaskDef aTaskDef) {
        mDatabase.insert(aTaskDef);
    }

    public boolean isFrameworkIdKnown(String aFrameworkId) {
        return mDatabase.sql("SELECT count(*) FROM Schedulers WHERE framework_id = ?", aFrameworkId).results(Long.class).get(0) > 0;
    }

    public Long getNumQueuedTasks(String aFrameworkId) {
        final List<Long> results = mDatabase.sql("SELECT count(*) FROM tasks WHERE allocated = false AND framework_id = ?", aFrameworkId).results(Long.class);
        return results.get(0);
    }

    public Long getNumActiveTasks(String aFrameworkId) {
        final List<Long> results = mDatabase.sql("SELECT count(*) FROM tasks WHERE allocated = true AND framework_id = ?", aFrameworkId).results(Long.class);
        return results.get(0);
    }

    public void registerScheduler(String aFrameworkId) {
        mDatabase.sql("INSERT INTO Schedulers VALUES (?, ?, ?)", aFrameworkId, ZonedDateTime.now(), false).execute();
    }


    public void recoverFramework(SchedulerFramework aSchedulerFramework) {

        mDatabase.sql(
                "UPDATE Schedulers SET recovering = true, recovered_time = ? WHERE framework_id = ? AND _seq_no = ? AND _primary_term = ?",
                ZonedDateTime.now(),
                aSchedulerFramework.framework_id,
                aSchedulerFramework._seq_no,
                aSchedulerFramework._primary_term).execute();
    }

    public void deregisterScheduler(String aFrameworkId) {
        mDatabase.sql("DELETE FROM Schedulers WHERE framework_id = ?", aFrameworkId).execute();
    }

}

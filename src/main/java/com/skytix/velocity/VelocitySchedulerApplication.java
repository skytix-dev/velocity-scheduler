package com.skytix.velocity;

import com.dieselpoint.norm.Database;
import com.skytix.schedulerclient.SchedulerRemote;
import com.skytix.velocity.scheduler.StandardSchedulerHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.springframework.beans.factory.annotation.Lookup;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@SpringBootApplication
@Slf4j
public class VelocitySchedulerApplication {

    public static void main(String[] aArgs) {
        ToStringBuilder.setDefaultStyle(ToStringStyle.DEFAULT_STYLE);

        new SpringApplicationBuilder(VelocitySchedulerApplication.class).web(WebApplicationType.SERVLET).run(aArgs);
        log.info("Velocity Scheduler is running");
    }

    @Bean
    @Primary
    public TaskScheduler taskExecutor() {
        final ThreadPoolTaskScheduler threadPoolScheduler = new ThreadPoolTaskScheduler();
        threadPoolScheduler.setPoolSize(4);

        return threadPoolScheduler;
    }

    @Bean
    public Database database() {
        final Database db = new Database();
        db.setJdbcUrl("jdbc:postgresql://172.20.170.179:5432/postgres");
        db.setUser("crate");

        return db;
    }

    @Lookup
    public SchedulerRemote schedulerRemote(StandardSchedulerHandler aEventHandler) {
        return aEventHandler.getSchedulerRemote();
    }

}

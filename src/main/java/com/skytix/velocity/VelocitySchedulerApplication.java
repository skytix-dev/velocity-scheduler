package com.skytix.velocity;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@SpringBootApplication
@Log4j2
public class VelocitySchedulerApplication {

    public static void main(String[] aArgs) {
        ToStringBuilder.setDefaultStyle(ToStringStyle.DEFAULT_STYLE);

        new SpringApplicationBuilder(VelocitySchedulerApplication.class).web(WebApplicationType.SERVLET).run(aArgs);
        log.info("Velocity Scheduler is running");
    }

    @Bean
    @Primary
    public TaskExecutor taskExecutor() {
        final ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();

        taskExecutor.setMaxPoolSize(4);

        return taskExecutor;
    }

}

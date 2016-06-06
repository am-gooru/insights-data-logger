
package org.logger.event.web.thread.config;

import java.util.concurrent.Executor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableAsync
public class AppConfig {

	@Bean(name = "threadPoolTaskExecutorForController")
	public Executor threadPoolTaskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(10);
		executor.setMaxPoolSize(45);
		executor.setQueueCapacity(150);
		executor.setThreadNamePrefix("eventExecutor");
		executor.initialize();
		return executor;
	}

}

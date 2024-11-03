package com.example.zookepertest.demo.listner;

import com.example.zookepertest.demo.service.TaskManagerService;
import com.example.zookepertest.demo.service.impl.TaskManagerServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TaskAllocationListener implements ApplicationListener<ContextRefreshedEvent> {

  private final TaskManagerService taskManagerService;

  @Autowired
  public TaskAllocationListener(TaskManagerServiceImpl taskManagerService) {
    this.taskManagerService = taskManagerService;
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent event) {
    log.info("Spring context refreshed. Triggering initial task allocation...");
    taskManagerService.assignedTasks();
  }
}

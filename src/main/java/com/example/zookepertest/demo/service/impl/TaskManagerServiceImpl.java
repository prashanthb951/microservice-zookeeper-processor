package com.example.zookepertest.demo.service.impl;

import com.example.zookepertest.demo.config.ZooKeeperConfiguration;
import com.example.zookepertest.demo.service.TaskService;
import com.example.zookepertest.demo.service.TaskManagerService;
import jakarta.annotation.PreDestroy;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TaskManagerServiceImpl implements TaskManagerService {

  private final com.example.zookepertest.demo.service.ZooKeeperOperationsService zooKeeperOperations;
  private final String myInstanceId = UUID.randomUUID().toString();
  private List<String> assignedTasks;
  private final ZooKeeperConfiguration zooKeeperConfiguration;
  private final ZooKeeperOperationsServiceImpl zooKeeperService;
  private ExecutorService executor;
  private final TaskService taskService;


  @Autowired
  public TaskManagerServiceImpl(
      com.example.zookepertest.demo.service.ZooKeeperOperationsService zooKeeperOperations,
      ZooKeeperConfiguration zooKeeperConfiguration,
      ZooKeeperOperationsServiceImpl zooKeeperService, 
      TaskService taskService) {
    this.zooKeeperOperations = zooKeeperOperations;
    this.zooKeeperConfiguration = zooKeeperConfiguration;
    this.zooKeeperService = zooKeeperService;
    this.taskService = taskService;
    registerInstance();
  }
  private void registerInstance() {
    try {
      String allocationNodePath = zooKeeperConfiguration.getWorkersPath() + "/" + myInstanceId + "||";
      zooKeeperOperations.createEphemeralSequentialNode(allocationNodePath, "".getBytes());
      log.info("Registered instance {} at: {}", myInstanceId, allocationNodePath);
    } catch (Exception e) {
      log.error("Error registering instance: {}", e.getMessage(), e);
    }
  }
  synchronized List<String> acquireTasks() throws Exception {
    List<String> availableTasks =
        zooKeeperOperations.getChildren(zooKeeperConfiguration.getTaskPath());
    if (availableTasks.isEmpty()) {
      return Collections.emptyList();
    }
    List<String> instances = zooKeeperOperations.getChildren(zooKeeperConfiguration.getWorkersPath());
    Collections.sort(instances);

    Integer tempinstanceIndex = -1;
    for (int i = 0; i < instances.size(); i++) {
      if (instances.get(i).substring(0, instances.get(i).indexOf("||")).equals(myInstanceId)) {
        tempinstanceIndex = i;
        break;
      }
    }
    final int instanceIndex = tempinstanceIndex;

    List<String> assigned = availableTasks.stream()
        .filter(task -> (availableTasks.indexOf(task) + 1) % instances.size() ==
            instanceIndex)
        .collect(Collectors.toList());

    for (String task : assigned) {
      String lockPath = zooKeeperConfiguration.getLocksPath() + "/" + task;
      InterProcessMutex mutex =
          new InterProcessMutex(zooKeeperService.getCuratorFramework(), lockPath);
      if (!mutex.acquire(10, TimeUnit.SECONDS)) {
        log.error("Could not acquire lock for task: {}", task);
        return null;
      }
      zooKeeperService.createEphemeralSequentialNode(lockPath + "/" + myInstanceId, "".getBytes());
    }
    return assigned;
  }
  @Override
  public void releaseAcquiredTasks()  {
    if (assignedTasks != null && !assignedTasks.isEmpty()) {
      for (String task : assignedTasks) {
        String lockPath = ZKPaths.makePath(zooKeeperConfiguration.getLocksPath(), task);
        try {
          zooKeeperOperations.deleteNode(lockPath);
          log.info("Released lock for task: {}", task);
        } catch (Exception e) {
          log.error("Error releasing lock for task {}: {}", task, e.getMessage(), e);
        }
      }
    }
  }

  private void startTaskProcessing() {
    executor = Executors.newSingleThreadExecutor();
    taskService.stopTaskProcessing();
    executor.submit(() -> taskService.processTasks(assignedTasks));
  }
  @Override
  public synchronized void assignedTasks() {
    try {
      stopProcessing();
      releaseAcquiredTasks();
      assignedTasks = acquireTasks();
      if (assignedTasks != null && !assignedTasks.isEmpty()) {
        log.info("Task service instance {} assigned tasks: {}", myInstanceId, assignedTasks);
        startTaskProcessing();
      } else {
        log.warn(
            "Task service instance {} could not acquire any Tasks. Retrying in 5 seconds.",
            myInstanceId);
        TimeUnit.SECONDS.sleep(5);
      }
    } catch (Exception e) {
      log.error("Error assiging tasks: {}", e.getMessage(), e);
    }
  }
  private void stopProcessing() {
    taskService.stopTaskProcessing();
  if (executor != null) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
          executor.shutdownNow();
          log.warn("Executor did not terminate in a timely manner. Forcibly shutting down.");
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
        log.error("Executor shutdown interrupted: {}", e.getMessage(), e);
      }
    }
  }

  @PreDestroy
  public void shutdown() {
    stopProcessing();
    try {
      releaseAcquiredTasks();
    } catch (Exception e) {
      log.error("Error releasing tasks on shutdown: {}", e.getMessage(), e);
    }
  }
}

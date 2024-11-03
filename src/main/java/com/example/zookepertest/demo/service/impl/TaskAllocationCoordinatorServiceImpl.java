package com.example.zookepertest.demo.service.impl;

import com.example.zookepertest.demo.config.ZooKeeperConfiguration;
import com.example.zookepertest.demo.service.TaskManagerService;
import com.example.zookepertest.demo.service.ZooKeeperOperationsService;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TaskAllocationCoordinatorServiceImpl {
  private final TaskManagerService taskManagerService;
  private final ZooKeeperConfiguration zooKeeperConfiguration;
  private final PathChildrenCache locksCache;
  private final PathChildrenCache cartProcessorsCache;
  private final PathChildrenCache marketsAddedCache;

  @Autowired
  public TaskAllocationCoordinatorServiceImpl(ZooKeeperOperationsService zooKeeperOperations,
                                                ZooKeeperConfiguration zooKeeperConfiguration,
                                                TaskManagerServiceImpl taskManagerService) {
    this.taskManagerService = taskManagerService;
    this.zooKeeperConfiguration = zooKeeperConfiguration;
    CuratorFramework curatorFramework = zooKeeperOperations.getCuratorFramework();
    locksCache =
        new PathChildrenCache(curatorFramework, zooKeeperConfiguration.getLocksPath(), true);
    cartProcessorsCache = new PathChildrenCache(curatorFramework, "/cart-processors", true);
    marketsAddedCache =
        new PathChildrenCache(curatorFramework, zooKeeperConfiguration.getTaskPath(), true);
    try {
      locksCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
      cartProcessorsCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
      marketsAddedCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    } catch (Exception e) {
      log.error("Error starting caches: {}", e.getMessage(), e);
    }
    locksCache.getListenable().addListener(this::handleLockEvent);
    cartProcessorsCache.getListenable().addListener(this::handleProcessorEvent);
    marketsAddedCache.getListenable().addListener(this::handleMarketEvent);
  }

  private void handleLockEvent(CuratorFramework client, PathChildrenCacheEvent event) {
    if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
      String task =
          event.getData().getPath().substring(zooKeeperConfiguration.getLocksPath().length() + 1);
      log.info("Lock released for task: {}", task);
    }
  }

  private void handleProcessorEvent(CuratorFramework client, PathChildrenCacheEvent event) {
    if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED ||
        event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
      log.info("Task processor change detected. Re-allocating tasks.");
      triggerMarketReAllocation();
    }
  }

  private void handleMarketEvent(CuratorFramework client, PathChildrenCacheEvent event) {
    if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED ||
        event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
      log.info("Task change detected. Re-allocating Tasks.");
      triggerMarketReAllocation();
    }
  }

  private void triggerMarketReAllocation() {
    try {
      log.info("Triggering task re-allocation...");
      taskManagerService.assignedTasks();
    } catch (Exception e) {
      log.error("Error during task re-allocation: {}", e.getMessage(), e);
    }
  }

  @PreDestroy
  void close() throws IOException {
    try {
      if (locksCache != null) {
        locksCache.close();
      }
      if (cartProcessorsCache != null) {
        cartProcessorsCache.close();
      }
      if (marketsAddedCache != null) {
        marketsAddedCache.close();
      }
    } catch (Exception e) {
      log.error("Error closing caches: {}", e.getMessage(), e);
    }
  }
}

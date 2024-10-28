package com.example.zookepertest.demo.service.impl;

import com.example.zookepertest.demo.config.ZooKeeperConfiguration;
import com.example.zookepertest.demo.service.CartService;
import com.example.zookepertest.demo.service.MarketManagerService;
import jakarta.annotation.PreDestroy;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MarketManagerServiceImpl implements MarketManagerService {

  private final com.example.zookepertest.demo.service.ZooKeeperOperationsService zooKeeperOperations;
  private final String myInstanceId = UUID.randomUUID().toString();
  private List<String> assignedMarkets;
  private final ZooKeeperConfiguration zooKeeperConfiguration;
  private static final String ALLOCATION_PATH = "/cart-processors";
  private final ZooKeeperOperationsServiceImpl zooKeeperService;
  private ExecutorService executor;
  private final AtomicBoolean processing = new AtomicBoolean(false);
  private final CartService cartService;


  @Autowired
  public MarketManagerServiceImpl(
      com.example.zookepertest.demo.service.ZooKeeperOperationsService zooKeeperOperations,
      ZooKeeperConfiguration zooKeeperConfiguration,
      ZooKeeperOperationsServiceImpl zooKeeperService, CartServiceImpl cartService) {
    this.zooKeeperOperations = zooKeeperOperations;
    this.zooKeeperConfiguration = zooKeeperConfiguration;
    this.zooKeeperService = zooKeeperService;
    this.cartService = cartService;
    registerInstance();
  }
  private void registerInstance() {
    try {
      String allocationNodePath = ALLOCATION_PATH + "/" + myInstanceId + "||";
      zooKeeperOperations.createEphemeralSequentialNode(allocationNodePath, "".getBytes());
      log.info("Registered instance {} at: {}", myInstanceId, allocationNodePath);
    } catch (Exception e) {
      log.error("Error registering instance: {}", e.getMessage(), e);
    }
  }
  synchronized List<String> acquireMarkets() throws Exception {
    List<String> availableMarkets =
        zooKeeperOperations.getChildren(zooKeeperConfiguration.getMarketPath());
    if (availableMarkets.isEmpty()) {
      return Collections.emptyList();
    }
    List<String> instances = zooKeeperOperations.getChildren(ALLOCATION_PATH);
    Collections.sort(instances);

    Integer tempinstanceIndex = -1;
    for (int i = 0; i < instances.size(); i++) {
      if (instances.get(i).substring(0, instances.get(i).indexOf("||")).equals(myInstanceId)) {
        tempinstanceIndex = i;
        break;
      }
    }
    final int instanceIndex = tempinstanceIndex;

    List<String> assigned = availableMarkets.stream()
        .filter(market -> (availableMarkets.indexOf(market) + 1) % instances.size() ==
            instanceIndex)
        .collect(Collectors.toList());

    for (String market : assigned) {
      String lockPath = zooKeeperConfiguration.getLocksPath() + "/" + market;
      InterProcessMutex mutex =
          new InterProcessMutex(zooKeeperService.getCuratorFramework(), lockPath);
      if (!mutex.acquire(10, TimeUnit.SECONDS)) {
        log.error("Could not acquire lock for market: {}", market);
        return null;
      }
      zooKeeperService.createEphemeralSequentialNode(lockPath + "/" + myInstanceId, "".getBytes());
    }
    return assigned;
  }
  @Override
  public void releaseAcquiredMarkets()  {
    if (assignedMarkets != null && !assignedMarkets.isEmpty()) {
      for (String market : assignedMarkets) {
        String lockPath = ZKPaths.makePath(zooKeeperConfiguration.getLocksPath(), market);
        try {
          zooKeeperOperations.deleteNode(lockPath);
          log.info("Released lock for market: {}", market);
        } catch (Exception e) {
          log.error("Error releasing lock for market {}: {}", market, e.getMessage(), e);
        }
      }
    }
  }

  private void startMarketProcessing() {
    executor = Executors.newSingleThreadExecutor();
    processing.set(true);
    executor.submit(() -> cartService.processMarkets(assignedMarkets));
  }
  @Override
  public synchronized void assignMarket() {
    try {
      stopProcessing();
      releaseAcquiredMarkets();
      assignedMarkets = acquireMarkets();
      if (assignedMarkets != null && !assignedMarkets.isEmpty()) {
        log.info("Cart service instance {} assigned markets: {}", myInstanceId, assignedMarkets);
        startMarketProcessing();
      } else {
        log.warn(
            "Cart service instance {} could not acquire any markets. Retrying in 5 seconds.",
            myInstanceId);
        TimeUnit.SECONDS.sleep(5);
      }
    } catch (Exception e) {
      log.error("Error assigning markets: {}", e.getMessage(), e);
    }
  }
  private void stopProcessing() {
    processing.set(false);
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
      releaseAcquiredMarkets();
    } catch (Exception e) {
      log.error("Error releasing market on shutdown: {}", e.getMessage(), e);
    }
  }
}

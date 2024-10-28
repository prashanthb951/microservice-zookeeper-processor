package com.example.zookepertest.demo.service.impl;

import com.example.zookepertest.demo.config.ZooKeeperConfiguration;
import com.example.zookepertest.demo.service.ZooKeeperOperationsService;
import jakarta.annotation.PreDestroy;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ZooKeeperOperationsServiceImpl
    implements ZooKeeperOperationsService {

  private final CuratorFramework curatorFramework;

  @Autowired
  public ZooKeeperOperationsServiceImpl(ZooKeeperConfiguration zooKeeperConfig) {
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
    curatorFramework = CuratorFrameworkFactory.newClient(zooKeeperConfig.getConnectString(), zooKeeperConfig.getSessionTimeoutMs(), zooKeeperConfig.getConnectionTimeoutMs(), retryPolicy);
    curatorFramework.start();
  }

  @Override
  public void createEphemeralSequentialNode(String path, byte[] data) throws Exception {
    curatorFramework.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(path, data);
  }

  @Override
  public List<String> getChildren(String path) throws Exception {
    return curatorFramework.getChildren().forPath(path);
  }

  @Override
  public CuratorFramework getCuratorFramework() {
    return curatorFramework;
  }

  @Override
  public void deleteNode(String path) {
    try {
      List<String> children = curatorFramework.getChildren().forPath(path);
      for (String child : children) {
        curatorFramework.delete().guaranteed().forPath(ZKPaths.makePath(path, child));
      }
      curatorFramework.delete().guaranteed().forPath(path);
    } catch (Exception e) {
      log.error("Error deleting node {}: {}", path, e.getMessage(), e);
    }
  }

  @PreDestroy
  public void close() {
    try {
      if (curatorFramework != null) curatorFramework.close();
    } catch (Exception e) {
      log.error("Error closing ZooKeeper resources: {}", e.getMessage(), e);
    }
  }
}

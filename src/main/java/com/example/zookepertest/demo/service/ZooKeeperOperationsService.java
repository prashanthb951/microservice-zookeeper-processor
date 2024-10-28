package com.example.zookepertest.demo.service;

import java.util.List;
import org.apache.curator.framework.CuratorFramework;

public interface ZooKeeperOperationsService {
  void createEphemeralSequentialNode(String path, byte[] data) throws Exception;
  List<String> getChildren(String path) throws Exception;
  void deleteNode(String path);
  CuratorFramework getCuratorFramework();
}

package com.example.zookepertest.demo.controller;

import com.example.zookepertest.demo.dto.ZkNode;
import com.example.zookepertest.demo.service.impl.ZooKeeperOperationsServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/zookeeper")
public class ZooKeeperController {

  @Autowired
  private ZooKeeperOperationsServiceImpl zooKeeperService;

  @PostMapping
  public ResponseEntity<Void> createNode(@RequestBody ZkNode node) {
    try {
      zooKeeperService.createEphemeralSequentialNode(node.getNodePath(), node.getData().getBytes());
      return ResponseEntity.ok().build();
    } catch (Exception e) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
  }
  @DeleteMapping
  public ResponseEntity<Void> deleteNode(@RequestParam String nodePath) {
    try {
      zooKeeperService.deleteNode(nodePath);
      return ResponseEntity.ok().build();
    } catch (Exception e) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
  }

}
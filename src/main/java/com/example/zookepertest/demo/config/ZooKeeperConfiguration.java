package com.example.zookepertest.demo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "zookeeper")
@Data
public class ZooKeeperConfiguration {
  private String connectString;
  private int sessionTimeoutMs;
  private int connectionTimeoutMs;
  private String locksPath;
  private String marketPath;
}

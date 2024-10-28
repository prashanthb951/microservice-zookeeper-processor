package com.example.zookepertest.demo.listner;

import com.example.zookepertest.demo.service.MarketManagerService;
import com.example.zookepertest.demo.service.impl.MarketManagerServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MarketAllocationListener implements ApplicationListener<ContextRefreshedEvent> {

  private final MarketManagerService marketManagerService;

  @Autowired
  public MarketAllocationListener(MarketManagerServiceImpl marketManagerService) {
    this.marketManagerService = marketManagerService;
  }

  @Override
  public void onApplicationEvent(ContextRefreshedEvent event) {
    log.info("Spring context refreshed. Triggering initial market allocation...");
    marketManagerService.assignMarket();
  }
}

package com.example.zookepertest.demo.service.impl;

import com.example.zookepertest.demo.dto.Cart;
import com.example.zookepertest.demo.repository.CartRepository;
import com.example.zookepertest.demo.service.TaskService;
import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TaskServiceImpl implements TaskService {
  private final CartRepository cartRepository;
  private final AtomicBoolean processing = new AtomicBoolean(false);

  @Autowired
  public TaskServiceImpl(CartRepository cartRepository) {
    this.cartRepository = cartRepository;
  }

  @PostConstruct
  public void init() {
    addSampleData();
  }

  private void addSampleData() {
    cartRepository.save(new Cart("aus", "Cart 1"));
    cartRepository.save(new Cart("usa", "Cart 2"));
    cartRepository.save(new Cart("zaf", "Cart 3"));
    cartRepository.save(new Cart("aus", "Cart 4"));
  }
  @Override
  public void processTasks(List<String> assignedTasks) {
    if (assignedTasks != null && processing.get()) {
      for (String task : assignedTasks) {
        if (!processing.get()) {
          log.info("Processing of tasks stopped because reassignment is in progress.");
          return;
        }
        processTask(task);
      }
    }
  }
     @Override
    public void stopTaskProcessing()
      {
         this.processing.set(false);
   }

  private void processTask(String task) {

    //Business Logic
    try {
      List<Cart> cartsToDelete = cartRepository.findByMarket(task);
      if (cartsToDelete != null && !cartsToDelete.isEmpty()) {
        log.info("Deleting {} Cart for market: {}", cartsToDelete.size(), task);
        for (Cart cart : cartsToDelete) {
          try {
            cartRepository.delete(cart);
            log.info("Deleted Cart: {}", cart.getId());
          } catch (Exception e) {
            log.error("Error deleting Cart {}: {}", cart.getId(), e.getMessage(), e);
          }
        }
      } else {
        log.info("No Carts found for task: {}", task);
      }
    } catch (Exception e) {
      log.error("Error processing task {}: {}", task, e.getMessage(), e);
    }
  }

}

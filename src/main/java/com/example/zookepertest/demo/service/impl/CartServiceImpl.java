package com.example.zookepertest.demo.service.impl;

import com.example.zookepertest.demo.dto.Cart;
import com.example.zookepertest.demo.repository.CartRepository;
import com.example.zookepertest.demo.service.CartService;
import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class CartServiceImpl implements CartService {
  private final CartRepository cartRepository;
  private final AtomicBoolean processing = new AtomicBoolean(false);

  @Autowired
  public CartServiceImpl(CartRepository cartRepository) {
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
  public void processMarkets(List<String> assignedMarkets) {
    if (assignedMarkets != null && processing.get()) {
      for (String market : assignedMarkets) {
        if (!processing.get()) {
          log.info("Processing of markets stopped because reassignment is in progress.");
          return;
        }
        processMarket(market);
      }
    }
  }

  private void processMarket(String market) {
    try {
      List<Cart> cartsToDelete = cartRepository.findByMarket(market);
      if (cartsToDelete != null && !cartsToDelete.isEmpty()) {
        log.info("Deleting {} Cart for market: {}", cartsToDelete.size(), market);
        for (Cart cart : cartsToDelete) {
          try {
            cartRepository.delete(cart);
            log.info("Deleted Cart: {}", cart.getId());
          } catch (Exception e) {
            log.error("Error deleting Cart {}: {}", cart.getId(), e.getMessage(), e);
          }
        }
      } else {
        log.info("No Carts found for market: {}", market);
      }
    } catch (Exception e) {
      log.error("Error processing market {}: {}", market, e.getMessage(), e);
    }
  }

}

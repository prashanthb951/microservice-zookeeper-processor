package com.example.zookepertest.demo.repository;

import com.example.zookepertest.demo.dto.Cart;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.stereotype.Repository;

@Repository
public class CartRepository {

  private final Map<Long, Cart> carts = new HashMap<>();
  private long nextId = 1;

  public Cart save(Cart cart) {
    cart.setId(nextId++);
    carts.put(cart.getId(), cart);
    return cart;
  }


  public List<Cart> findByMarket(String market) {
    return carts.values().stream()
        .filter(p -> market.equals(p.getMarket()))
        .collect(Collectors.toList());
  }

  public void delete(Cart cart) {
    carts.remove(cart.getId());
  }

  //Add a method to get all carts
  public List<Cart> findAll() {
    return new ArrayList<>(carts.values());
  }
}

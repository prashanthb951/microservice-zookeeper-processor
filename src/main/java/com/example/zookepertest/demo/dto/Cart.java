package com.example.zookepertest.demo.dto;

import lombok.Data;

@Data
public class Cart {

  private Long id;
  private String market;
  private String cartDetails;

  public Cart(String market, String cartDetails) {
    this.market = market;
    this.cartDetails = cartDetails;
  }
}

package com.zengcode.eip.camel.ep4.aggregatation.model;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.List;

@Getter
@Setter
public class Order {
    public String orderId;
    public List<Item> items;

    @Getter
    @Setter
    public static class Item {
        public String sku;
        public int qty;
        public BigDecimal price;
    }
}

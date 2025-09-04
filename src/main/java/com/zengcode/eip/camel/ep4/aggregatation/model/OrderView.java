package com.zengcode.eip.camel.ep4.aggregatation.model;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Map;

@Getter
@Setter
public class OrderView {
    public String orderId;
    public Map<String, Boolean> availabilityBySku;
    public BigDecimal subtotal, tax, grandTotal;
}

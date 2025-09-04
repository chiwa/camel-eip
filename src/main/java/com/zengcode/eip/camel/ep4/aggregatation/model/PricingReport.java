package com.zengcode.eip.camel.ep4.aggregatation.model;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
public class PricingReport {
    public String orderId;
    public BigDecimal subtotal, tax, grandTotal;
}

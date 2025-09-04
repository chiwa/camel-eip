package com.zengcode.eip.camel.ep4.aggregatation.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class InventoryReport {
    public String orderId;
    public Map<String, Boolean> availabilityBySku;
}
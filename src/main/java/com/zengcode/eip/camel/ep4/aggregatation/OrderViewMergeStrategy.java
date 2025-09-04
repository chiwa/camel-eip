package com.zengcode.eip.camel.ep4.aggregatation;

import com.zengcode.eip.camel.ep4.aggregatation.model.InventoryReport;
import com.zengcode.eip.camel.ep4.aggregatation.model.OrderView;
import com.zengcode.eip.camel.ep4.aggregatation.model.PricingReport;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;


public class OrderViewMergeStrategy implements AggregationStrategy {

    @Override public Exchange aggregate(Exchange oldEx, Exchange newEx) {
        Object b = newEx.getMessage().getBody();
        if (oldEx == null) {
            var v = new OrderView();
            if (b instanceof InventoryReport i) { v.orderId = i.orderId; v.availabilityBySku = i.availabilityBySku; }
            if (b instanceof PricingReport p) { v.orderId = p.orderId; v.subtotal = p.subtotal; v.tax = p.tax; v.grandTotal = p.grandTotal; }
            newEx.getMessage().setBody(v); return newEx;
        }
        OrderView v = oldEx.getMessage().getBody(OrderView.class);
        if (b instanceof InventoryReport i) v.availabilityBySku = i.availabilityBySku;
        if (b instanceof PricingReport   p) { v.subtotal = p.subtotal; v.tax = p.tax; v.grandTotal = p.grandTotal; }
        oldEx.getMessage().setBody(v); return oldEx;
    }
}
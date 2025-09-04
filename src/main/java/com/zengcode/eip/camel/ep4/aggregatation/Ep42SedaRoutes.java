package com.zengcode.eip.camel.ep4.aggregatation;

import com.zengcode.eip.camel.ep4.aggregatation.model.InventoryReport;
import com.zengcode.eip.camel.ep4.aggregatation.model.Order;
import com.zengcode.eip.camel.ep4.aggregatation.model.PricingReport;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.context.annotation.Profile; import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
@Profile("ep4-aggregator-seda")
public class Ep42SedaRoutes extends RouteBuilder {

    @Override public void configure() {
        // รับ order JSON ก้อนเดียว (เดโมให้ง่าย): ตั้ง header orderId แล้วยิงออก 2 สาย
        from("seda:orders.in")
                .routeId("orders-intake-seda")
                .unmarshal().json(JsonLibrary.Jackson, Order.class)
                .setHeader("orderId", simple("${body.orderId}"))
                .multicast().parallelProcessing()
                .to("seda:inventory", "seda:pricing")
                .end();
        from("seda:inventory")
                .routeId("inventory-seda")
                .process(e -> {
                    Order o = e.getMessage().getBody(Order.class);
                    Map<String, Boolean> map = new LinkedHashMap<>();
                    o.items.forEach(it -> map.put(it.sku, true));
                    InventoryReport r = new InventoryReport(); r.orderId = o.orderId; r.availabilityBySku = map; e.getMessage().setBody(r);
                })
                .to("seda:agg.in");
        from("seda:pricing")
                .routeId("pricing-seda")
                .process(e -> {
                    Order o = e.getMessage().getBody(Order.class);
                    java.math.BigDecimal sub = java.math.BigDecimal.ZERO;
                    for (Order.Item it : o.items) {
                        java.math.BigDecimal unit = it.price != null ? it.price : java.math.BigDecimal.ZERO;
                        sub = sub.add(unit.multiply(java.math.BigDecimal.valueOf(it.qty)));
                    }
                    PricingReport pr = new PricingReport();
                    pr.orderId = o.orderId; pr.subtotal = sub; pr.tax = sub.multiply(new java.math.BigDecimal("0.07")); pr.grandTotal = pr.subtotal.add(pr.tax);
                    e.getMessage().setBody(pr);
                })
                .to("seda:agg.in");
        from("seda:agg.in")
                .routeId("aggregate-seda")
                .aggregate(header("orderId"), new OrderViewMergeStrategy())
                .completionSize(2)      // รอสองสาย: inventory + pricing
                .completionTimeout(5000)
                .marshal().json(JsonLibrary.Jackson)
                .to("seda:ready");
    }
}
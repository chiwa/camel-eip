package com.zengcode.eip.camel.ep4.splitter;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"ep4-splitter", "kafka"})
public class SplitterRoute extends RouteBuilder {

    @Override
    public void configure() {

        // Producer: รับ JSON Array แล้วแตกเป็นหลาย message
        from("direct:orders.split.in")
                .routeId("splitter-producer")
                // รับเป็น JSON array string -> แปลงเป็น List<Map> ก่อน
                .unmarshal().json()
                // split รายการย่อย ๆ (ถ้าเป็น nested ใช้ .jsonpath())
                .split(body())
                .marshal().json() // ให้แต่ละ message เป็น JSON object
                .to("kafka:orders.split.items")
                .end();

        // Subscriber #1: inventory
        from("kafka:orders.split.items?groupId=inventory-svc&autoOffsetReset=earliest")
                .routeId("splitter-inventory")
                .to("seda:orders.inventory.split");

        // Subscriber #2: analytics
        from("kafka:orders.split.items?groupId=analytics-svc&autoOffsetReset=earliest")
                .routeId("splitter-analytics")
                .to("seda:orders.analytics.split");
    }
}

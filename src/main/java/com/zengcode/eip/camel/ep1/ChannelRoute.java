package com.zengcode.eip.camel.ep1;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"ep1","kafka"})
public class ChannelRoute extends RouteBuilder {
    @Override public void configure() {
        from("direct:orderCreated")
                .routeId("order-created-publisher")
                .to("kafka:orders.created");

// ✅ ใส่ autoOffsetReset=earliest ให้ route-consumer ด้วย
        from("kafka:orders.created?groupId=inventory-svc&autoOffsetReset=earliest")
                .routeId("inventory-subscriber")
                .to("kafka:inventory.inbox");

        from("kafka:orders.created?groupId=payment-svc&autoOffsetReset=earliest")
                .routeId("payment-subscriber")
                .to("kafka:payment.inbox");
    }
}
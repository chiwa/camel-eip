package com.zengcode.eip.camel.ep3;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"ep3-event","kafka"})
public class EventMessageRoute extends RouteBuilder {
    @Override
    public void configure() {
        // Producer → Kafka (fire-and-forget)
        from("direct:orders.event.in")
                .routeId("event-producer")
                .to("kafka:orders.events");

        // Inventory subscriber
        from("kafka:orders.events?groupId=inventory-svc&autoOffsetReset=earliest")
                .routeId("event-inventory")
                .to("seda:orders.events.inventory");

        // Analytics subscriber
        from("kafka:orders.events?groupId=analytics-svc&autoOffsetReset=earliest")
                .routeId("event-analytics")
                .to("seda:orders.events.analytics");
    }
}
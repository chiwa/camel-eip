package com.zengcode.eip.camel.ep2;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"ep2-pubsub","kafka"})
public class PubSubRoute extends RouteBuilder {
    @Override
    public void configure() {
        // Producer → ส่ง OrderCreated
        from("direct:orders.pubsub.in")
                .routeId("pubsub-producer")
                .to("kafka:orders.pubsub");

        // Inventory subscriber
        from("kafka:orders.pubsub?groupId=inventory-svc&autoOffsetReset=earliest")
                .routeId("pubsub-inventory")
                .to("seda:orders.inventory.pubsub");

        // Analytics subscriber
        from("kafka:orders.pubsub?groupId=analytics-svc&autoOffsetReset=earliest")
                .routeId("pubsub-analytics")
                .to("seda:orders.analytics.pubsub");

        // Notification subscriber
        from("kafka:orders.pubsub?groupId=notification-svc&autoOffsetReset=earliest")
                .routeId("pubsub-notification")
                .to("seda:orders.notification.pubsub");
    }
}
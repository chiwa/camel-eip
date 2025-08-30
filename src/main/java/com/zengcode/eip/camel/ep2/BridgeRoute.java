package com.zengcode.eip.camel.ep2;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("ep2-bridge & kafka & rabbit")
public class BridgeRoute extends RouteBuilder {
    @Override
    public void configure() {
        // Producer (Legacy) → Spring RabbitMQ
        from("direct:legacy.orders.in")
                .routeId("bridge-producer-rabbit")
                .to("spring-rabbitmq:legacy.orders"
                        + "?routingKey=legacy.orders"
                        + "&queues=legacy.orders"
                        + "&autoDeclare=true");

        // BRIDGE: Spring RabbitMQ → Kafka
        from("spring-rabbitmq:legacy.orders"
                + "?routingKey=legacy.orders"
                + "&queues=legacy.orders"
                + "&autoDeclare=true")
                .routeId("bridge-rabbit-to-kafka")
                .to("kafka:bridge.orders");

        // Sink เพื่อ assert
        from("kafka:bridge.orders?groupId=bridge-assert&autoOffsetReset=earliest")
                .routeId("bridge-kafka-sink")
                .to("seda:bridge.orders.sink");
    }
}

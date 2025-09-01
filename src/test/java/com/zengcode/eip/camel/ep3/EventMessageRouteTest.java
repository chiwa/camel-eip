package com.zengcode.eip.camel.ep3;

import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;


@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@ActiveProfiles({"ep3-event","kafka"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class EventMessageRouteTest {

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("camel.component.kafka.brokers", kafka::getBootstrapServers);
    }

    @Autowired
    ProducerTemplate producer;
    @Autowired
    ConsumerTemplate consumer;

    @Test
    void broadcast_event_and_both_subscribers_receive() {
        String evt = """
        {"eventType":"OrderCreated","eventVersion":"1",
         "data":{"orderId":"ORD-20001","amount":999.0}}
        """;

        // ส่ง Event
        producer.sendBody("direct:orders.event.in", evt);

        // ทั้ง Inventory และ Analytics ต้องได้รับ
        String inv = consumer.receiveBody("seda:orders.events.inventory", 10000, String.class);
        String ana = consumer.receiveBody("seda:orders.events.analytics", 10000, String.class);

        assertThat(inv).isNotNull().contains("\"OrderCreated\"");
        assertThat(ana).isNotNull().contains("\"OrderCreated\"");
    }
}
package com.zengcode.eip.camel.ep2;

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
@ActiveProfiles({"ep2-pubsub","kafka"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PubSubRouteTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry r) {
        r.add("camel.component.kafka.brokers", kafka::getBootstrapServers);
    }

    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;

    @Test
    void orderCreated_broadcasts_to_all_subscribers() {
        var order = """
      {"orderId":"ORD-5001","amount":1200}
      """;

        producer.sendBody("direct:orders.pubsub.in", order);

        String inv  = consumer.receiveBody("seda:orders.inventory.pubsub",   10000, String.class);
        String ana  = consumer.receiveBody("seda:orders.analytics.pubsub",   10000, String.class);
        String noti = consumer.receiveBody("seda:orders.notification.pubsub",10000, String.class);

        assertThat(inv).isNotNull().contains("ORD-5001");
        assertThat(ana).isNotNull().contains("ORD-5001");
        assertThat(noti).isNotNull().contains("ORD-5001");
    }
}
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
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@ActiveProfiles({"ep2-bridge","kafka","rabbit"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class BridgeRouteTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static RabbitMQContainer rabbit = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:3.13-management"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("camel.component.kafka.brokers", kafka::getBootstrapServers);
        r.add("spring.rabbitmq.host", rabbit::getHost);
        r.add("spring.rabbitmq.port", rabbit::getAmqpPort);
        r.add("spring.rabbitmq.username", rabbit::getAdminUsername);
        r.add("spring.rabbitmq.password", rabbit::getAdminPassword);
    }

    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;

    @Test
    void rabbit_messages_are_forwarded_to_kafka() {
        var order = """
      {"orderId":"ORD-9001","amount":1500}
      """;

        // ส่งเข้าฝั่ง Legacy (RabbitMQ)
        producer.sendBody("direct:legacy.orders.in", order);

        // ควรหลุดจากสะพานไปถึง Kafka แล้วไหลเข้า SEDA sink ให้เรา assert ได้
        String got = consumer.receiveBody("seda:bridge.orders.sink", 10000, String.class);
        assertThat(got).isNotNull().contains("ORD-9001");
    }
}
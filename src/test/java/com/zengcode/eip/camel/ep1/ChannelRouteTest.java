package com.zengcode.eip.camel.ep1;

import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.NotifyBuilder;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@ActiveProfiles({"ep1","kafka"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ChannelRouteTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    // ให้ Camel ใช้ brokers จาก Testcontainers
    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("camel.component.kafka.brokers", kafka::getBootstrapServers);
    }

    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;
    @Autowired
    CamelContext camel;

    @Test
    void orderCreated_fanout_to_inventory_and_payment() {

        var done = new NotifyBuilder(camel).whenDone(1).create();

        var payload = """
          {"orderId":"ORD-1001","amount":250}
          """;

        // ส่งเข้าทาง in-memory endpoint ที่ route ผูกไว้
        producer.sendBody("direct:orderCreated", payload);
        //producer.sendBody("kafka:orders.created", payload);
        // จริงๆ เราส่งไปหา kafka ตรงๆ ก็ได้นะ แต่อยากทำ endpoint ที่เป็น in-memory ให้ดู

        // รอ route ทำงานให้เสร็จก่อนค่อย assert (กัน timing issue)
        assertThat(done.matches(10, TimeUnit.SECONDS)).isTrue();

        // อ่านด้วย group เฉพาะของการ assert + reset เป็น earliest
        String inv = consumer.receiveBody(
                "kafka:inventory.inbox?groupId=assert-inventory&autoOffsetReset=earliest",
                10000, String.class);

        String pay = consumer.receiveBody(
                "kafka:payment.inbox?groupId=assert-payment&autoOffsetReset=earliest",
                10000, String.class);

        assertThat(inv).isNotNull().contains("ORD-1001");
        assertThat(pay).isNotNull().contains("ORD-1001");
    }
}
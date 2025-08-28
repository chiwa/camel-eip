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

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@ActiveProfiles({"ep1", "kafka"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TranslatorRouteTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    // ให้ Camel ใช้ brokers จาก Testcontainers
    @DynamicPropertySource
    static void camelProps(DynamicPropertyRegistry r) {
        r.add("camel.component.kafka.brokers", kafka::getBootstrapServers);
    }

    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;
    @Autowired CamelContext camel;

    @Test
    void json_translates_to_xml_for_billing() {
        // รอ 1 exchange ให้เสร็จ (รับจาก customer.json → ส่งต่อ billing.xml)
        var done = new NotifyBuilder(camel).whenDone(1).create();

        var json = """
      {"customerId":"C-42","name":"Alice","plan":"PREMIUM"}
      """;
        // ส่งเข้า topic ต้นทางของ translator
        producer.sendBody("kafka:customer.json", json);

        // กัน timing; ถ้า local เสถียรจะผ่านโดยไม่ต้องรอ แต่ใน CI แนะนำให้คงไว้
        assertThat(done.matches(10, TimeUnit.SECONDS)).isTrue();

        // อ่านด้วย group ของการ assert + เริ่มจาก earliest
        String xml = consumer.receiveBody(
                "kafka:billing.xml?groupId=assert-billing&autoOffsetReset=earliest",
                10000, String.class);

        assertThat(xml)
                .isNotNull()
                .contains("<billingCustomer>")
                .contains("<customerId>C-42</customerId>")
                .contains("<name>Alice</name>")
                .contains("<plan>PREMIUM</plan>");
    }
}
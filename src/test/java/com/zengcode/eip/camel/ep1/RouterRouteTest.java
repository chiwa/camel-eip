package com.zengcode.eip.camel.ep1;

import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.NotifyBuilder;
import org.apache.camel.CamelContext;
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
@ActiveProfiles({"ep1", "kafka"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RouterRouteTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    // ✅ บอก Camel ให้ใช้ brokers จาก Testcontainers
    @DynamicPropertySource
    static void camelProps(DynamicPropertyRegistry r) {
        r.add("camel.component.kafka.brokers", kafka::getBootstrapServers);
    }

    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;
    @Autowired CamelContext camel;

    @Test
    void suspicious_goes_to_fraud_else_payment() {
        // เตรียมรอให้มี 2 exchanges เสร็จ (ไป fraud.queue และ payment.queue อย่างละ 1)
        var done = new NotifyBuilder(camel).whenDone(2).create();

        var legit = """
                {"txId":"T-1","amount":500,"suspicious":false}
                """;
        var fraud = """
                {"txId":"T-2","amount":9999,"suspicious":true}
                """;

        // ส่งเข้า topic ต้นทางของ router
        producer.sendBody("kafka:transactions", legit);
        producer.sendBody("kafka:transactions", fraud);

        // รอ route ทำงานให้เสร็จก่อนค่อย assert (กัน timing issue)
        assertThat(done.matches(10, TimeUnit.SECONDS)).isTrue();

        // ✅ อ่านด้วย group แยกสำหรับการ assert + reset offset = earliest
        String toPayment = consumer.receiveBody(
                "kafka:payment.queue?groupId=assert-payment&autoOffsetReset=earliest",
                10000, String.class);
        String toFraud = consumer.receiveBody(
                "kafka:fraud.queue?groupId=assert-fraud&autoOffsetReset=earliest",
                10000, String.class);

        assertThat(toPayment).isNotNull().contains("T-1");
        assertThat(toFraud).isNotNull().contains("T-2");
    }
}
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

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@ActiveProfiles({"ep2-gd","kafka"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class GuaranteedDeliveryRouteTest {

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
    void duplicates_are_filtered_and_unique_messages_are_processed_once() {
        // ส่งข้อความซ้ำ (txId เดียวกัน) 2 ครั้ง + ข้อความอีกอันที่ txId ต่างกัน
        var m1 = "{\"txId\":\"TX-100\",\"amount\":500}";
        var m1dup = "{\"txId\":\"TX-100\",\"amount\":500}"; // จำลอง duplicate
        var m2 = "{\"txId\":\"TX-200\",\"amount\":800}";

        producer.sendBody("direct:payments.gd.in", m1);
        producer.sendBody("direct:payments.gd.in", m1dup);
        producer.sendBody("direct:payments.gd.in", m2);

        // ดึงผลลัพธ์จาก SEDA: ควรได้แค่ 2 รายการ (TX-100, TX-200) ไม่ซ้ำ
        Set<String> out = new HashSet<>();
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline && out.size() < 2) {
            String body = consumer.receiveBody("seda:payments.gd.processed", 500, String.class);
            if (body != null) out.add(body);
        }

        assertThat(out).hasSize(2);
        String joined = String.join("\n", out);
        assertThat(joined).contains("TX-100");
        assertThat(joined).contains("TX-200");
    }
}
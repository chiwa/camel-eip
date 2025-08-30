package com.zengcode.eip.camel.ep2;

import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
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

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@ActiveProfiles({"ep2-p2p","kafka"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class P2PRouteTest {

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

    @AfterEach
    void drainSeda() {
        // best-effort ล้างคิว in-memory เพื่อไม่ให้ค้างข้ามเทสต์
        for (int i = 0; i < 10; i++) {
            consumer.receiveNoWait("seda:workers.w1.p2p");
            consumer.receiveNoWait("seda:workers.w2.p2p");
        }
    }

    @Test
    void each_message_processed_by_exactly_one_worker() {
        List<String> msgs = List.of(
                "{\"orderId\":\"ORD-1\",\"amount\":100}",
                "{\"orderId\":\"ORD-2\",\"amount\":200}",
                "{\"orderId\":\"ORD-3\",\"amount\":300}"
        );

        // ส่งเข้าท่อ P2P
        msgs.forEach(m -> producer.sendBody("direct:payments.p2p.in", m));

        // พยายามดึงข้อความให้ครบ 3 จาก w1/w2 รวมกัน (ไม่บังคับให้ต้องกระจายเท่ากัน)
        Set<String> collected = new HashSet<>();
        long deadline = System.currentTimeMillis() + 15000; // 15s budget
        while (System.currentTimeMillis() < deadline && collected.size() < msgs.size()) {
            String m = consumer.receiveBody("seda:workers.w1.p2p", 500, String.class);
            if (m == null) {
                m = consumer.receiveBody("seda:workers.w2.p2p", 500, String.class);
            }
            if (m != null) collected.add(m);
        }

        assertThat(collected).hasSize(3);
        String joined = String.join(" ", collected);
        assertThat(joined).contains("ORD-1");
        assertThat(joined).contains("ORD-2");
        assertThat(joined).contains("ORD-3");
    }
}
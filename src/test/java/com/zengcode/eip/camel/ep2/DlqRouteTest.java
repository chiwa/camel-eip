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
@ActiveProfiles({"ep2-dlq", "kafka"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DlqRouteTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry r) {
        r.add("camel.component.kafka.brokers", kafka::getBootstrapServers);
    }

    @Autowired
    ProducerTemplate producer;
    @Autowired
    ConsumerTemplate consumer;

    @Test
    void invalid_payload_goes_to_dlq_after_retries() {
        var bad = """
                {"subject":"Hello"}
                """; // missing 'to'

        producer.sendBody("direct:emails.request.in", bad);

        // ไม่ควรไปถึง emails.sent เลย
        String ok = consumer.receiveBody("seda:emails.sent", 2000, String.class);
        assertThat(ok).as("invalid payload should not be delivered to sent queue").isNull();

        // ควรไปถึง DLQ หลัง retry ล้มเหลว
        String dlq = consumer.receiveBody(
                "kafka:emails.dlq?groupId=assert-dlq&autoOffsetReset=earliest",
                10000, String.class);
        assertThat(dlq).isNotNull();
    }

    @Test
    void valid_payload_is_processed_normally() {
        var good = """
                {"to":"alice@example.com","subject":"Welcome","body":"Hi Alice"}
                """;

        producer.sendBody("direct:emails.request.in", good);

        String sent = consumer.receiveBody("seda:emails.sent", 10000, String.class);
        assertThat(sent)
                .isNotNull()
                .contains("TO=alice@example.com")
                .contains("SUB=Welcome")
                .contains("BODY=Hi Alice");
    }
}
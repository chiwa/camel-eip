package com.zengcode.eip.camel.ep4.splitter;

import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@ActiveProfiles({"ep4-splitter", "kafka"})
class SplitterFlowTest {

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(
                    DockerImageName.parse("confluentinc/cp-kafka:7.6.1")
                            .asCompatibleSubstituteFor("apache/kafka")
            );

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("camel.component.kafka.brokers", kafka::getBootstrapServers);
    }

    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;

    @Test
    void split_json_array_and_fanout_to_two_subscribers() {
        // JSON array ของ order lines (ตัวอย่าง)
        String jsonArray = """
            [
              {"lineId":1,"sku":"A-100","qty":2},
              {"lineId":2,"sku":"B-200","qty":3}
            ]
            """;

        // ส่งเข้า producer
        producer.sendBody("direct:orders.split.in", jsonArray);

        // แต่ละ subscriber (inventory / analytics) ควรได้รับ 2 ข้อความ
        String inv1 = consumer.receiveBody("seda:orders.inventory.split", 20000, String.class);
        String inv2 = consumer.receiveBody("seda:orders.inventory.split", 20000, String.class);
        String an1  = consumer.receiveBody("seda:orders.analytics.split", 20000, String.class);
        String an2  = consumer.receiveBody("seda:orders.analytics.split", 20000, String.class);

        assertThat(inv1).isNotNull();
        assertThat(inv2).isNotNull();
        assertThat(an1).isNotNull();
        assertThat(an2).isNotNull();

        // เช็คเนื้อหาอย่างหยาบ ๆ ให้เห็นว่าได้แต่ละรายการจริง
        assertThat(inv1 + inv2).contains("\"sku\":\"A-100\"");
        assertThat(inv1 + inv2).contains("\"sku\":\"B-200\"");

        assertThat(an1 + an2).contains("\"sku\":\"A-100\"");
        assertThat(an1 + an2).contains("\"sku\":\"B-200\"");
    }
}


package com.zengcode.eip.camel.ep3.requestreply;

import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;

@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@ActiveProfiles({"ep3-reqreply","kafka"})
class RpcFlowTest {

    static final Network net = Network.newNetwork();

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(
                    DockerImageName.parse("confluentinc/cp-kafka:7.6.1")
                            .asCompatibleSubstituteFor("apache/kafka")
            ).withNetwork(net)
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7.2"))
                    .withNetwork(net)
                    .withNetworkAliases("redis")
                    .withExposedPorts(6379);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("camel.component.kafka.brokers", kafka::getBootstrapServers);
        r.add("spring.data.redis.host", redis::getHost);
        r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    PaymentsRpcGateway gateway;

    @Test
    void rpc_end_to_end() {
        Map<String, Object> req = Map.of(
                "txId", "TX-123",
                "amount", 1500
        );

        String json = gateway.call(req, Duration.ofSeconds(20));

        org.assertj.core.api.Assertions.assertThat(json)
                .contains("\"status\":\"OK\"")
                .contains("\"txId\":\"TX-123\"");
    }
}
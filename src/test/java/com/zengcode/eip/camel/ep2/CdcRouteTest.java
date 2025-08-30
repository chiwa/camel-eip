package com.zengcode.eip.camel.ep2;

import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@ActiveProfiles({"ep2-cdc","kafka"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class CdcRouteTest {

    // ให้ทุกคอนเทนเนอร์คุยกันด้วย alias บน network เดียวกัน
    static final Network net = Network.newNetwork();

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withNetwork(net)
            .withNetworkAliases("kafka"); // ห้าม override ENV ของ KafkaContainer

    @Container
    static GenericContainer<?> postgres = new GenericContainer<>(DockerImageName.parse("debezium/postgres:16"))
            .withNetwork(net)
            .withNetworkAliases("postgres")
            .withEnv("POSTGRES_USER", "test")
            .withEnv("POSTGRES_PASSWORD", "test")
            .withEnv("POSTGRES_DB", "testdb")
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());

    @Container
    static GenericContainer<?> connect = new GenericContainer<>(DockerImageName.parse("debezium/connect:2.6"))
            .withNetwork(net)
            .withNetworkAliases("connect")
            .dependsOn(kafka, postgres)
            // ชี้ Kafka ภายใน network
            .withEnv("BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv("GROUP_ID", "connect-cluster")
            .withEnv("CONFIG_STORAGE_TOPIC", "connect-configs")
            .withEnv("OFFSET_STORAGE_TOPIC", "connect-offsets")
            .withEnv("STATUS_STORAGE_TOPIC", "connect-status")
            // ให้ REST พร้อมก่อนเริ่มเทส
            .withExposedPorts(8083)
            .waitingFor(
                    Wait.forHttp("/connectors")
                            .forStatusCode(200)
                            .withStartupTimeout(Duration.ofSeconds(90))
            );

    @DynamicPropertySource
    static void camelProps(DynamicPropertyRegistry r) {
        // ให้ Camel ใช้ brokers ของ KafkaContainer (สำหรับฝั่ง host/app)
        r.add("camel.component.kafka.brokers", kafka::getBootstrapServers);
    }

    @Autowired ConsumerTemplate consumer;
    @Autowired ProducerTemplate producer;

    @Test
    void postgres_changes_flow_to_kafka_via_debezium_and_reach_camel_route() throws Exception {
        // 1) เตรียมตารางจาก host ผ่าน mapped port
        String jdbcUrl = "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432) + "/testdb";
        try (Connection c = DriverManager.getConnection(jdbcUrl, "test", "test");
             Statement st = c.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                  id INT PRIMARY KEY,
                  name TEXT,
                  email TEXT
                );
            """);
            st.execute("TRUNCATE customers;");
        }

        // 2) สมัคร Debezium connector (ปิด snapshot เพื่อให้ event แรกเป็น op:"c")
        int connectPort = connect.getMappedPort(8083);
        String connectorConfigJson = """
        {
          "name": "customers-connector",
          "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "test",
            "database.password": "test",
            "database.dbname": "testdb",

            "topic.prefix": "dbserver1",
            "plugin.name": "pgoutput",
            "schema.include.list": "public",
            "table.include.list": "public.customers",
            "tombstones.on.delete": "false",
            "slot.name": "customers_slot",
            "publication.autocreate.mode": "filtered",

            "snapshot.mode": "never",

            "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
            "schema.history.internal.kafka.topic": "schema-changes.testdb",

            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false"
          }
        }
        """;

        /**
         * สมัคร Debezium Postgres Connector เข้า Kafka Connect
         * เพื่อบอกว่า “เวลา Postgres customers table มีการเปลี่ยนแปลง
         * → ให้ส่ง CDC events ไปที่ Kafka topic dbserver1.public.customers”.
         */
        var http = HttpClient.newHttpClient();
        var create = HttpRequest.newBuilder()
                .uri(URI.create("http://" + connect.getHost() + ":" + connectPort + "/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(connectorConfigJson))
                .build();
        var createResp = http.send(create, HttpResponse.BodyHandlers.ofString());
        assertThat(createResp.statusCode()).isIn(201, 409);

        // รอให้ connector + task RUNNING จริง
        Awaitility.await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> {
            var statusReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + connect.getHost() + ":" + connectPort + "/connectors/customers-connector/status"))
                    .GET().build();
            var statusResp = http.send(statusReq, HttpResponse.BodyHandlers.ofString());
            assertThat(statusResp.statusCode()).isEqualTo(200);
            String body = statusResp.body();
            assertThat(body).contains("\"state\":\"RUNNING\"");
            assertThat(body).contains("\"tasks\":[");
            assertThat(body).contains("\"id\":0");
            assertThat(body).contains("\"state\":\"RUNNING\"");
        });

        // 3) ทำการเปลี่ยนข้อมูล → Debezium ควรปล่อย event "c" ไป topic dbserver1.public.customers
        try (Connection c = DriverManager.getConnection(jdbcUrl, "test", "test");
             Statement st = c.createStatement()) {
            st.execute("INSERT INTO customers (id, name, email) VALUES (101, 'Chiwa Kantawong', 'kchiwa@gmail.com');");
        }

        // 4) ดึงจาก SEDA sink (route: kafka -> seda) แล้ว assert
        String event = consumer.receiveBody("seda:cdc.customers.sink", 120_000, String.class);
        assertThat(event).isNotNull();
        assertThat(event).contains("\"op\":\"c\"");
        assertThat(event).contains("\"after\":{\"id\":101");
        assertThat(event).contains("Chiwa Kantawong");
        assertThat(event).contains("kchiwa@gmail.com");
    }
}
package com.zengcode.eip.camel.ep4.routeslip;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.Exchange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@SpringBootTest
@ActiveProfiles("ep4-slip-seda")
class Ep44RoutingSlipRoutesTest {

    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;

    private static final ObjectMapper om = new ObjectMapper();

    @Test
    void dynamicFlow_followValidateEnrichDeliver_inOrder() throws Exception {
        // Arrange: header slip = validate -> enrich -> deliver
        Map<String, Object> headers = new HashMap<>();
        headers.put("slip", "direct:validate,direct:enrich,direct:deliver");

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("orderId", "ORD-1001");

        // Act
        producer.sendBodyAndHeaders("seda:in.slip", body, headers);

        // Assert
        String out = consumer.receiveBody("seda:done", 4000, String.class);
        Assertions.assertNotNull(out, "Should receive final output on seda:done");
        JsonNode root = om.readTree(out);
        JsonNode steps = root.path("steps");
        Assertions.assertTrue(steps.isArray(), "steps must be an array");
        Assertions.assertEquals(3, steps.size(), "steps should contain 3 entries");
        Assertions.assertEquals("validate", steps.get(0).asText());
        Assertions.assertEquals("enrich",   steps.get(1).asText());
        Assertions.assertEquals("deliver",  steps.get(2).asText());
    }

    @Test
    void dynamicFlow_canSkipEnrich_validateThenDeliver() throws Exception {
        // Arrange: header slip = validate -> deliver (ข้าม enrich)
        Map<String, Object> headers = new HashMap<>();
        headers.put("slip", "direct:validate,direct:deliver");

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("orderId", "ORD-1002");

        // Act
        producer.sendBodyAndHeaders("seda:in.slip", body, headers);

        // Assert
        String out = consumer.receiveBody("seda:done", 4000, String.class);
        Assertions.assertNotNull(out, "Should receive final output on seda:done");
        JsonNode root = om.readTree(out);
        JsonNode steps = root.path("steps");
        Assertions.assertEquals(2, steps.size(), "steps should contain 2 entries when enrich is skipped");
        Assertions.assertEquals("validate", steps.get(0).asText());
        Assertions.assertEquals("deliver",  steps.get(1).asText());
    }

    @Test
    void dynamicFlow_customOrder_deliverOnly() throws Exception {
        // Arrange: ส่งตรงไป deliver อย่างเดียว
        Map<String, Object> headers = new HashMap<>();
        headers.put("slip", "direct:deliver");

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("orderId", "ORD-1003");

        // Act
        producer.sendBodyAndHeaders("seda:in.slip", body, headers);

        // Assert
        Exchange ex = consumer.receive("seda:done", 4000);
        Assertions.assertNotNull(ex, "Should receive exchange on seda:done");
        String out = ex.getMessage().getBody(String.class);
        JsonNode steps = om.readTree(out).path("steps");
        Assertions.assertEquals(1, steps.size(), "Only one step when routing slip has a single endpoint");
        Assertions.assertEquals("deliver", steps.get(0).asText());
    }
}
package com.zengcode.eip.camel.ep4.processmanager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
@ActiveProfiles("ep4-processmanager-seda")
class Ep45ProcessManagerRoutesTest {
    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;
    private static final ObjectMapper om = new ObjectMapper();
    @Test
    void success_allSteps_doneWithCompletedStatus() throws Exception {
        Map<String, Object> order = new LinkedHashMap<>();
        order.put("orderId", "ORD-5001");
        producer.sendBody("seda:pm.start", order);
        String out = consumer.receiveBody("seda:pm.done", 4000, String.class);
        Assertions.assertNotNull(out, "should get success output");
        JsonNode root = om.readTree(out);
        Assertions.assertEquals("COMPLETED", root.path("status").asText());
        List<String> steps = om.convertValue(root.path("steps"), List.class);
        Assertions.assertEquals(List.of(
                "reserve-inventory", "process-payment", "arrange-shipping", "completed"
        ), steps);
    }
    @Test
    void failAtPayment_triggerRefundAndCancelInventory_toCompensated() throws Exception {
        Map<String, Object> order = new LinkedHashMap<>();
        order.put("orderId", "ORD-5002");
        order.put("failAt", "payment");
        producer.sendBody("seda:pm.start", order);
        String out = consumer.receiveBody("seda:pm.compensated", 4000, String.class);
        Assertions.assertNotNull(out, "should get compensated output");
        JsonNode root = om.readTree(out);
        Assertions.assertEquals("COMPENSATED", root.path("status").asText());
        List<String> steps = om.convertValue(root.path("steps"), List.class);
        Assertions.assertEquals(List.of(
                "reserve-inventory", "process-payment",
                "refund-payment", "cancel-inventory"
        ), steps);
    }
    @Test
    void failAtShipping_triggerCancelShippingRefundPaymentCancelInventory() throws Exception {
        Map<String, Object> order = new LinkedHashMap<>();
        order.put("orderId", "ORD-5003");
        order.put("failAt", "shipping");
        producer.sendBody("seda:pm.start", order);
        String out = consumer.receiveBody("seda:pm.compensated", 4000, String.class);
        Assertions.assertNotNull(out, "should get compensated output");
        JsonNode root = om.readTree(out);
        Assertions.assertEquals("COMPENSATED", root.path("status").asText());
        List<String> steps = om.convertValue(root.path("steps"), List.class);
        Assertions.assertEquals(List.of(
                "reserve-inventory", "process-payment", "arrange-shipping",
                "cancel-shipping", "refund-payment", "cancel-inventory"
        ), steps);
    }
}
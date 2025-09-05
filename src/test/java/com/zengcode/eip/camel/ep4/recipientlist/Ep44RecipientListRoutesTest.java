package com.zengcode.eip.camel.ep4.recipientlist;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("ep4-recipient-seda")
class Ep44RecipientListRoutesTest {

    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;

    private static final ObjectMapper om = new ObjectMapper();

    @Test
    void fixedFanOut_sendToEmailAndAudit() throws Exception {
        // Arrange
        String in = "{\"orderId\":\"ORD-1\",\"payload\":\"hello\"}";

        // Act
        producer.sendBody("seda:in.fixed", in);

        // Assert
        String outEmail = consumer.receiveBody("seda:out.email", 3000, String.class);
        String outAudit = consumer.receiveBody("seda:out.audit", 3000, String.class);
        Assertions.assertNotNull(outEmail, "email output should exist");
        Assertions.assertNotNull(outAudit, "audit output should exist");

        JsonNode je = om.readTree(outEmail);
        JsonNode ja = om.readTree(outAudit);
        Assertions.assertTrue(je.path("steps").isArray());
        Assertions.assertTrue(ja.path("steps").isArray());
        Assertions.assertEquals("email", je.path("steps").get(0).asText());
        Assertions.assertEquals("audit", ja.path("steps").get(0).asText());
    }
}
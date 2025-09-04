package com.zengcode.eip.camel.ep4.aggregation;

import org.apache.camel.ProducerTemplate;
import org.apache.camel.ConsumerTemplate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;

@SpringBootTest
@ActiveProfiles("ep4-aggregator-seda")
class Ep42SedaIT {

    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;

    private static final ObjectMapper om = new ObjectMapper();

    @Test
    void aggregate_twoBranches_onSingleOrder() throws Exception {
        // Arrange
        String ord = """
          {"orderId":"ORD-1001","items":[
            {"sku":"COFFEE","qty":2,"price":120.00},
            {"sku":"BAGEL","qty":1,"price":85.00}
          ]}
        """;

        // Act
        producer.sendBody("seda:orders.in", ord);
        String out = consumer.receiveBody("seda:ready", 5000, String.class);

        // Assert: มีผลลัพธ์
        Assertions.assertNotNull(out, "Should receive aggregated output within timeout");

        // Parse JSON เพื่อตรวจค่าแบบจริงจัง
        JsonNode root = om.readTree(out);

        // 1) โครงสร้างหลัก
        Assertions.assertEquals("ORD-1001", root.path("orderId").asText(), "orderId must match");
        Assertions.assertTrue(root.has("availabilityBySku"), "availabilityBySku should be present");
        Assertions.assertTrue(root.has("grandTotal"), "grandTotal should be present");

        // 2) availability ครบ 2 ชิ้น และเป็น true
        JsonNode avail = root.path("availabilityBySku");
        Assertions.assertTrue(avail.has("COFFEE"), "COFFEE availability missing");
        Assertions.assertTrue(avail.has("BAGEL"), "BAGEL availability missing");
        Assertions.assertTrue(avail.get("COFFEE").asBoolean(), "COFFEE should be available");
        Assertions.assertTrue(avail.get("BAGEL").asBoolean(), "BAGEL should be available");
        Assertions.assertEquals(2, avail.size(), "Should have exactly 2 availability entries");

        // 3) ตรวจยอดเงิน (2*120 + 1*85 = 325.00), tax 7% = 22.75, grandTotal = 347.75
        BigDecimal expectedSubtotal = new BigDecimal("325.00");
        BigDecimal expectedTax = new BigDecimal("22.75");
        BigDecimal expectedGrand = new BigDecimal("347.75");

        BigDecimal actualSubtotal = root.get("subtotal").decimalValue();
        BigDecimal actualTax = root.get("tax").decimalValue();
        BigDecimal actualGrand = root.get("grandTotal").decimalValue();

        Assertions.assertEquals(0, expectedSubtotal.compareTo(actualSubtotal), "subtotal mismatch");
        Assertions.assertEquals(0, expectedTax.compareTo(actualTax), "tax mismatch");
        Assertions.assertEquals(0, expectedGrand.compareTo(actualGrand), "grandTotal mismatch");

        // 4) ไม่ควรมีผลลัพธ์ซ้ำ (ไม่มีข้อความชุดที่สอง)
        String extra = consumer.receiveBody("seda:ready", 300, String.class);
        Assertions.assertNull(extra, "Should not receive duplicated aggregation output");
    }

    @Test
    void aggregate_twoOrders_inParallel_producesTwoDistinctResults() throws Exception {
        // Arrange: สองออเดอร์
        String ord1 = """
          {"orderId":"ORD-1001","items":[
            {"sku":"COFFEE","qty":2,"price":120.00},
            {"sku":"BAGEL","qty":1,"price":85.00}
          ]}
        """;
        String ord2 = """
          {"orderId":"ORD-1002","items":[
            {"sku":"JUICE","qty":3,"price":45.50},
            {"sku":"COOKIE","qty":5,"price":25.00}
          ]}
        """;

        // ค่าที่คาดหวังของ ORD-1002:
        // subtotal = (3 * 45.50) + (5 * 25.00) = 136.50 + 125.00 = 261.50
        // tax 7%  = 18.305
        // grand   = 279.805
        BigDecimal expectedSubtotal2 = new BigDecimal("261.50");
        BigDecimal expectedTax2 = new BigDecimal("18.305");
        BigDecimal expectedGrand2 = new BigDecimal("279.805");

        // Act
        producer.sendBody("seda:orders.in", ord1);
        producer.sendBody("seda:orders.in", ord2);

        String outA = consumer.receiveBody("seda:ready", 7000, String.class);
        String outB = consumer.receiveBody("seda:ready", 7000, String.class);

        Assertions.assertNotNull(outA, "First aggregated output should arrive");
        Assertions.assertNotNull(outB, "Second aggregated output should arrive");

        JsonNode r1 = om.readTree(outA);
        JsonNode r2 = om.readTree(outB);

        // ตรวจว่าเป็นคนละ orderId
        String id1 = r1.path("orderId").asText();
        String id2 = r2.path("orderId").asText();
        Assertions.assertNotEquals(id1, id2, "Two results must belong to different orders");

        // เลือกผลลัพธ์ที่เป็น ORD-1002 เพื่อตรวจค่าเงิน
        JsonNode r1002 = "ORD-1002".equals(id1) ? r1 : r2;

        Assertions.assertTrue(r1002.has("subtotal") && r1002.has("tax") && r1002.has("grandTotal"),
                "ORD-1002 must contain subtotal/tax/grandTotal");

        Assertions.assertEquals(0, expectedSubtotal2.compareTo(r1002.get("subtotal").decimalValue()), "subtotal (ORD-1002) mismatch");
        Assertions.assertEquals(0, expectedTax2.compareTo(r1002.get("tax").decimalValue()), "tax (ORD-1002) mismatch");
        Assertions.assertEquals(0, expectedGrand2.compareTo(r1002.get("grandTotal").decimalValue()), "grandTotal (ORD-1002) mismatch");
    }
}
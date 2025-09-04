package com.zengcode.eip.camel.ep4.resequencer;

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
@ActiveProfiles("ep4-resequencer-seda")
class Ep43ReSequenceRoutesTest {

    @Autowired ProducerTemplate producer;
    @Autowired ConsumerTemplate consumer;

    private static final ObjectMapper om = new ObjectMapper();

    @Test
    void singleOrder_outIsResequenced_1_2_3() throws Exception {
        // ส่ง step สลับลำดับ: 2,1,3
        producer.sendBody("seda:events.in", "{\"orderId\":\"ORD-1001\",\"step\":2,\"type\":\"X\"}");
        producer.sendBody("seda:events.in", "{\"orderId\":\"ORD-1001\",\"step\":1,\"type\":\"X\"}");
        producer.sendBody("seda:events.in", "{\"orderId\":\"ORD-1001\",\"step\":3,\"type\":\"X\"}");

        // ดึงผล 3 ชุด (เผื่อ timeout) — ควรเรียง 1,2,3
        String a = consumer.receiveBody("seda:ordered.out", 4000, String.class);
        String b = consumer.receiveBody("seda:ordered.out", 4000, String.class);
        String c = consumer.receiveBody("seda:ordered.out", 4000, String.class);

        int s1 = om.readTree(a).get("step").asInt();
        int s2 = om.readTree(b).get("step").asInt();
        int s3 = om.readTree(c).get("step").asInt();

        Assertions.assertEquals(1, s1);
        Assertions.assertEquals(2, s2);
        Assertions.assertEquals(3, s3);
    }

    @Test
    void twoOrders_interleaved_eachOrderResequencedIndependently() throws Exception {
        // ส่งสลับ order กันไปมา
        producer.sendBody("seda:events.in", "{\"orderId\":\"ORD-A\",\"step\":2}");
        producer.sendBody("seda:events.in", "{\"orderId\":\"ORD-B\",\"step\":1}");
        producer.sendBody("seda:events.in", "{\"orderId\":\"ORD-A\",\"step\":1}");
        producer.sendBody("seda:events.in", "{\"orderId\":\"ORD-B\",\"step\":2}");

        // รับออกมาทั้งหมด 4 รายการ (ภายใน 4 วิ)
        String o1 = consumer.receiveBody("seda:ordered.out", 4000, String.class);
        String o2 = consumer.receiveBody("seda:ordered.out", 4000, String.class);
        String o3 = consumer.receiveBody("seda:ordered.out", 4000, String.class);
        String o4 = consumer.receiveBody("seda:ordered.out", 4000, String.class);

        // รวม steps ต่อ order เพื่อตรวจว่าภายในแต่ละ order เรียง 1→2
        int[] aSteps = java.util.stream.Stream.of(o1,o2,o3,o4)
                .map(s -> parse(s))
                .filter(n -> n.get("orderId").asText().equals("ORD-A"))
                .mapToInt(n -> n.get("step").asInt())
                .sorted().toArray();

        int[] bSteps = java.util.stream.Stream.of(o1,o2,o3,o4)
                .map(s -> parse(s))
                .filter(n -> n.get("orderId").asText().equals("ORD-B"))
                .mapToInt(n -> n.get("step").asInt())
                .sorted().toArray();

        Assertions.assertArrayEquals(new int[]{1,2}, aSteps, "ORD-A should be 1,2 in order");
        Assertions.assertArrayEquals(new int[]{1,2}, bSteps, "ORD-B should be 1,2 in order");
    }

    private JsonNode parse(String s){
        try { return om.readTree(s); } catch (Exception e) { throw new RuntimeException(e); }
    }
}
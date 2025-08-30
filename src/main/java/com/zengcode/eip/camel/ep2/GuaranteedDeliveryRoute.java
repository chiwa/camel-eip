package com.zengcode.eip.camel.ep2;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.processor.idempotent.MemoryIdempotentRepository;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"ep2-gd","kafka"})
public class GuaranteedDeliveryRoute extends RouteBuilder {
    @Override
    public void configure() {
        // Producer: ปลอดภัยฝั่ง producer
        from("direct:payments.gd.in")
                .routeId("gd-producer")
                .to("kafka:payments.gd.requests?"
                        + "additionalProperties[enable.idempotence]=true&"
                        + "additionalProperties[acks]=all&"
                        + "additionalProperties[retries]=3");

        // Consumer: Idempotent (ใช้ txId เป็น key)
        from("kafka:payments.gd.requests?groupId=gd-workers&autoOffsetReset=earliest")
                .routeId("gd-consumer")
                .unmarshal().json()
                .idempotentConsumer(simple("${body[txId]}"),
                        MemoryIdempotentRepository.memoryIdempotentRepository(2000))
                .skipDuplicate(true)                 // ดรอปซ้ำ ไม่ไหลต่อ
                .to("seda:payments.gd.processed")    // ✅ ใส่ “ขั้นตอนลูก” ไว้ในบล็อก
                .end();
    }
}
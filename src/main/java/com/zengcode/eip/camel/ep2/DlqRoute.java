package com.zengcode.eip.camel.ep2;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"ep2-dlq","kafka"})
public class DlqRoute extends RouteBuilder {
    @Override
    public void configure() {
        // ✅ Global error handling: retry แล้วค่อยส่งเข้า DLQ
        onException(Exception.class)
                .maximumRedeliveries(3)           // retry รวม 3 ครั้ง
                .redeliveryDelay(0)               // เร็วในเทสต์
                .handled(true)                    // จบ error ที่นี่ ไม่ให้ตกไปที่ caller
                .to("kafka:emails.dlq");        // ส่งเข้า DLQ topic

        // Producer (เช่น API) ยิงคำขออีเมลเข้าท่อกลาง
        from("direct:emails.request.in")
                .routeId("emails-producer")
                .to("kafka:emails.request.dlq");

        // Consumer: Email service
        from("kafka:emails.request.dlq?groupId=email-svc&autoOffsetReset=earliest")
                .routeId("emails-consumer")
                .unmarshal().json()
                .process(ex -> {
                    var map = ex.getMessage().getBody(java.util.Map.class);
                    // validation แบบง่าย: ต้องมี 'to' และ 'subject'
                    if (map.get("to") == null || map.get("subject") == null) {
                        throw new IllegalArgumentException("invalid email payload: missing required fields");
                    }
                    // สมมติว่าเราสร้างข้อความอีเมลเรียบง่าย
                    String built = "TO=" + map.get("to") + "|SUB=" + map.get("subject") + "|BODY=" + map.getOrDefault("body", "");
                    ex.getMessage().setBody(built);
                })
                // ส่งต่อไปที่ in-memory sink เพื่อ assert ในเทสต์
                .to("seda:emails.sent");
    }
}
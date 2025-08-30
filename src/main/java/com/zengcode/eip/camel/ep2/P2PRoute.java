package com.zengcode.eip.camel.ep2;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"ep2-p2p","kafka"})
public class P2PRoute extends RouteBuilder {
    @Override
    public void configure() {
        // Producer (เช่น Payment API) → ท่อกลาง
        from("direct:payments.p2p.in")
                .routeId("p2p-producer")
                .to("kafka:payments.p2p.requests");

        // Worker #1 อยู่ในกลุ่มเดียวกัน → แข่งกับ worker #2
        from("kafka:payments.p2p.requests?groupId=payment-workers&autoOffsetReset=earliest")
                .routeId("p2p-worker-1")
                .setHeader("workerId").constant("w1")
                // ใช้ SEDA เป็น sink สำหรับ assert ในเทสต์
                .to("seda:workers.w1.p2p");

        // Worker #2 (กลุ่มเดียวกัน)
        from("kafka:payments.p2p.requests?groupId=payment-workers&autoOffsetReset=earliest")
                .routeId("p2p-worker-2")
                .setHeader("workerId").constant("w2")
                .to("seda:workers.w2.p2p");
    }
}
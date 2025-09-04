package com.zengcode.eip.camel.ep4.resequencer;

import com.zengcode.eip.camel.ep4.resequencer.model.OrderEvent;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.processor.resequencer.ExpressionResultComparator;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("ep4-resequencer-seda")
public class Ep43ReSequenceRoutes extends RouteBuilder {
    @Override
    public void configure() {
        from("seda:events.in")
                .routeId("events-intake-seda")
                .unmarshal().json(JsonLibrary.Jackson, OrderEvent.class)
                .setHeader("orderId", simple("${body.orderId}"))
                .setHeader("step",    simple("${body.step}"))
                // ทำ key แบบ zero-pad เพื่อให้เรียงสวยแม้เป็นสตริง
                .process(e -> {
                    var m = e.getMessage();
                    String orderId = m.getHeader("orderId", String.class);
                    Integer step   = m.getHeader("step", Integer.class);
                    int s = step != null ? step : 0;
                    m.setHeader("seqKey", String.format("%s:%06d", orderId, s));
                })
                .to("seda:resequencer.in");

        from("seda:resequencer.in")
                .routeId("resequencer-seda")
                .resequence(header("seqKey"))      // ใช้สตริงคีย์ได้ในโหมด batch
                    .batch()
                        .timeout(1500)                 // รวบ 1.5 วิแล้วค่อยปล่อยตามอันดับ
                        .ignoreInvalidExchanges()      // กันเคส header ขาด
                        .removeHeader("seqKey")
                        .marshal().json(JsonLibrary.Jackson)
                .to("seda:ordered.out");
    }
}
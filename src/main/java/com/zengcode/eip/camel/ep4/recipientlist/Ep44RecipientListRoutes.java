package com.zengcode.eip.camel.ep4.recipientlist;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@Profile("ep4-recipient-seda")
public class Ep44RecipientListRoutes extends RouteBuilder {

    @Override
    public void configure() {
        // Recipient List (fixed fan-out)
        from("seda:in.fixed")
                .routeId("ep44-fixed-recipient-list")
                .unmarshal().json(JsonLibrary.Jackson, Map.class)
                // ส่งไป email และ audit พร้อมกัน (ตัวอย่างแบบ fix)
                .setHeader("recipients", constant("seda:svc.email,seda:svc.audit"))
                .recipientList(header("recipients")).parallelProcessing();

        // ปลายทางจำลอง: email
        from("seda:svc.email")
                .routeId("ep44-svc-email")
                .process(ex -> appendStep(ex.getMessage(), "email"))
                .marshal().json(JsonLibrary.Jackson)
                .to("seda:out.email");

        // ปลายทางจำลอง: audit
        from("seda:svc.audit")
                .routeId("ep44-svc-audit")
                .process(ex -> appendStep(ex.getMessage(), "audit"))
                .marshal().json(JsonLibrary.Jackson)
                .to("seda:out.audit");
    }


    @SuppressWarnings("unchecked")
    private void appendStep(Message msg, String step) {
        Map<String, Object> src = msg.getBody(Map.class);

        // copy map (เลี่ยงเขียนทับ object ร่วม)
        Map<String, Object> map = (src == null) ? new LinkedHashMap<>() : new LinkedHashMap<>(src);

        // copy steps list (เลี่ยงแชร์ list เดิม)
        List<String> steps;
        Object existing = map.get("steps");
        if (existing instanceof List<?> l) {
            steps = new ArrayList<>((List<String>) l);
        } else {
            steps = new ArrayList<>();
        }

        steps.add(step);
        map.put("steps", steps);
        msg.setBody(map);
    }
}
package com.zengcode.eip.camel.ep4.routeslip;

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
@Profile("ep4-slip-seda")
public class Ep44RoutingSlipRoutes extends RouteBuilder {

    @Override
    public void configure() {
        // Routing Slip (dynamic)
        from("seda:in.slip")
                .routeId("ep44-routing-slip")
                .routingSlip(header("slip")); // header("slip") = "direct:validate,direct:enrich,direct:deliver"

        from("direct:validate")
                .routeId("ep44-step-validate")
                .process(ex -> appendStep(ex.getMessage(), "validate"));

        from("direct:enrich")
                .routeId("ep44-step-enrich")
                .process(ex -> appendStep(ex.getMessage(), "enrich"));

        from("direct:deliver")
                .routeId("ep44-step-deliver")
                .process(ex -> appendStep(ex.getMessage(), "deliver"))
                .marshal().json(JsonLibrary.Jackson)   // ✅ แปลงเป็น JSON ก่อนส่งออก
                .to("seda:done");
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
package com.zengcode.eip.camel.ep4.processmanager;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@Profile("ep4-processmanager-seda")
public class Ep45ProcessManagerRoutes extends RouteBuilder {
    @Override
    public void configure() {

        onException(Exception.class)
            .useOriginalMessage()
            .to("direct:compensate")
            .process(e -> {
                var m = e.getMessage();
                Map<String,Object> body = asMap(m.getBody());
                body.put("status", "COMPENSATED");
                // ใส่เหตุผลลง body ด้วย (ถ้าอยาก assert ในเทส)
                Exception ex = e.getProperty(org.apache.camel.Exchange.EXCEPTION_CAUGHT, Exception.class);
                if (ex != null) body.put("failReason", ex.getMessage());
                m.setBody(body);
            })
            .marshal().json(JsonLibrary.Jackson)
            .to("seda:pm.compensated")
            .handled(true);

        from("seda:pm.start")
            .routeId("ep45-pm-orchestrator")
            .process(e -> ensureBodyMap(e.getMessage()))
            .setHeader("orderId", simple("${body[orderId]}"))
            .to("direct:reserve")
            .to("direct:pay")
            .to("direct:ship")
            .process(e -> addStep(e.getMessage(), "completed"))
            .process(e -> {
                var m = e.getMessage();
                Map<String,Object> body = asMap(m.getBody());
                body.put("status", "COMPLETED");
                m.setBody(body);
            })
            .marshal().json(JsonLibrary.Jackson)
            .to("seda:pm.done");


        // บริการจำลอง: Reserve Inventory
        from("direct:reserve").routeId("ep45-step-reserve")
            .process(e -> addStep(e.getMessage(), "reserve-inventory"))
            .process(e -> maybeFail(e.getMessage(), "reserve"));

        // บริการจำลอง: Process Payment
        from("direct:pay").routeId("ep45-step-pay")
            .process(e -> addStep(e.getMessage(), "process-payment"))
            .process(e -> maybeFail(e.getMessage(), "payment"));

        // บริการจำลอง: Arrange Shipping
        from("direct:ship").routeId("ep45-step-ship")
            .process(e -> addStep(e.getMessage(), "arrange-shipping"))
            .process(e -> maybeFail(e.getMessage(), "shipping"));

        // Compensation: ย้อนกลับตาม step ที่สำเร็จก่อนหน้า
        from("direct:compensate").routeId("ep45-compensate")
            .process(e -> {
                var m = e.getMessage();
                Map<String, Object> body = asMap(m.getBody());
                List<String> steps = getSteps(body);
                if (steps.contains("arrange-shipping")) steps.add("cancel-shipping");
                if (steps.contains("process-payment")) steps.add("refund-payment");
                if (steps.contains("reserve-inventory")) steps.add("cancel-inventory");
                m.setBody(body);
            });
    }

    private void ensureBodyMap(org.apache.camel.Message msg) {
        Object b = msg.getBody();
        if (b instanceof Map<?,?>) return;
        msg.setBody(new LinkedHashMap<String,Object>());
    }
    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object b) {
        if (b instanceof Map<?,?> m) return (Map<String, Object>) m;
        return new LinkedHashMap<>();
    }
    @SuppressWarnings("unchecked")
    private void addStep(org.apache.camel.Message msg, String step) {
        Map<String, Object> body = asMap(msg.getBody());
        List<String> steps = (List<String>) body.get("steps");
        if (steps == null) { steps = new ArrayList<>(); body.put("steps", steps); }
        else { steps = new ArrayList<>(steps); body.put("steps", steps); }
        steps.add(step);
        msg.setBody(body);
    }
    private List<String> getSteps(Map<String, Object> body) {
        @SuppressWarnings("unchecked")
        List<String> steps = (List<String>) body.get("steps");
        if (steps == null) { steps = new ArrayList<>(); body.put("steps", steps); }
        return steps;
    }
    // ถ้ามี body.failAt == key → โยน exception เพื่อทริกเกอร์ compensate
    private void maybeFail(org.apache.camel.Message msg, String key) {
        Map<String, Object> body = asMap(msg.getBody());
        Object failAt = body.get("failAt");
        if (key.equals(failAt)) throw new IllegalStateException("forced failure at: " + key);
    }
}
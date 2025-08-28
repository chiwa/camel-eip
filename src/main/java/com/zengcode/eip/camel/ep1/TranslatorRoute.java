package com.zengcode.eip.camel.ep1;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"ep1", "kafka"})
public class TranslatorRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        from("kafka:customer.json?groupId=translator")
                .unmarshal().json()
                .process(ex -> {
                    var map = ex.getMessage().getBody(java.util.Map.class);
                    String xml = """
                            <billingCustomer>
                              <customerId>%s</customerId>
                              <name>%s</name>
                              <plan>%s</plan>
                            </billingCustomer>
                            """.formatted(map.get("customerId"), map.get("name"), map.get("plan"));
                    ex.getMessage().setBody(xml);
                })
                .to("kafka:billing.xml");
    }
}

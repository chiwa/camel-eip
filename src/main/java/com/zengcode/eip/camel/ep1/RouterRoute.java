package com.zengcode.eip.camel.ep1;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"ep1", "kafka"})
public class RouterRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        from("kafka:transactions?groupId=fraud-router&autoOffsetReset=earliest")
                .unmarshal().json()
                .choice()
                    .when(simple("${body[suspicious]} == true"))
                        .to("kafka:fraud.queue")
                    .otherwise()
                        .to("kafka:payment.queue");
    }
}

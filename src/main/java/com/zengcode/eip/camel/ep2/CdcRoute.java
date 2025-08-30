package com.zengcode.eip.camel.ep2;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"ep2-cdc & kafka"})
public class CdcRoute extends RouteBuilder {
    @Override
    public void configure() {
        // Debezium (serverName=dbserver1) จะตั้งชื่อ topic เป็น:
        //   <serverName>.<schema>.<table>  เช่น dbserver1.public.customers
        from("kafka:dbserver1.public.customers?groupId=cdc-assert&autoOffsetReset=earliest")
                .routeId("cdc-kafka-to-seda")
                .to("seda:cdc.customers.sink");
    }
}
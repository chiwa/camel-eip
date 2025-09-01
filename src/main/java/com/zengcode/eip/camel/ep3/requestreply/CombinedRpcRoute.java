package com.zengcode.eip.camel.ep3.requestreply;

import lombok.RequiredArgsConstructor;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

@Component
@Profile({"ep3-reqreply","kafka"})
@RequiredArgsConstructor
public class CombinedRpcRoute extends RouteBuilder {

    private final StringRedisTemplate redis;

    @Override
    public void configure() {

        // ให้เห็น error ชัด ๆ เวลาเทสต์
        onException(Exception.class)
                .handled(true)
                .log("!! RPC ERROR: ${exception.message}")
                .log("   headers=${headers} body=${body}");

        // ========= 1) Producer: direct -> Kafka (requests) =========
        from("direct:payments.rpc.request")
                .routeId("rpc-send")
                // ใช้ Processor ตั้ง header เอง (ไม่พึ่ง simple expression)
                .process(e -> {
                    String corr = e.getMessage().getHeader("correlationId", String.class);
                    if (corr == null || corr.isBlank()) {
                        corr = java.util.UUID.randomUUID().toString();
                        e.getMessage().setHeader("correlationId", corr);
                    }
                    e.getMessage().setHeader("replyTo", "payments.rpc.replies");
                })
                .log("Producer sending request corrId=${header.correlationId} replyTo=${header.replyTo} body=${body}")
                .marshal().json()
                .to("kafka:payments.rpc.requests");

        // ========= 2) Responder: Kafka (requests) -> Kafka (replies) =========
        from("kafka:payments.rpc.requests?groupId=rpc-responder&autoOffsetReset=earliest")
                .routeId("rpc-responder")
                .log("Responder got request corrId=${header.correlationId} rawBody=${body}")
                .unmarshal().json()
                .process(e -> {
                    Map<String, Object> req = e.getMessage().getBody(Map.class);

                    String corrId = e.getMessage().getHeader("correlationId", String.class);
                    String replyTo = e.getMessage().getHeader("replyTo", String.class);

                    if (replyTo == null || replyTo.isBlank()) {
                        throw new IllegalArgumentException("Missing replyTo header");
                    }
                    if (corrId == null || corrId.isBlank()) {
                        throw new IllegalArgumentException("Missing correlationId header");
                    }

                    Map<String, Object> reply = Map.of(
                            "txId", req.get("txId"),
                            "status", "OK",
                            "processedAt", java.time.Instant.now().toString()
                    );

                    e.getMessage().setBody(reply);
                    e.getMessage().setHeader("correlationId", corrId);
                    e.getMessage().setHeader("replyTo", replyTo);
                })
                .log("Responder sending reply corrId=${header.correlationId} to=${header.replyTo} body=${body}")
                .marshal().json()
                .toD("kafka:${header.replyTo}");

        // ========= 3) Reply Listener: Kafka (replies) -> Redis =========
        from("kafka:payments.rpc.replies?groupId=rpc-replies&autoOffsetReset=latest")
                .routeId("rpc-reply-listener")
                .log("ReplyListener got reply corrId=${header.correlationId} rawBody=${body}")
                .process(e -> {
                    String corr = e.getMessage().getHeader("correlationId", String.class);
                    String body = e.getMessage().getBody(String.class);

                    if (corr == null || corr.isBlank()) {
                        // กัน edge case ให้ไม่ทำให้เทสต์ล่มเงียบ ๆ
                        throw new IllegalArgumentException("Missing correlationId on reply message");
                    }

                    String key = "rpc:reply:" + corr;

                    // ปลุกฝั่งที่รอ (Gateway.rightPop)
                    redis.opsForList().leftPush(key, body);
                    redis.expire(key, Duration.ofMinutes(10));
                });
    }
}
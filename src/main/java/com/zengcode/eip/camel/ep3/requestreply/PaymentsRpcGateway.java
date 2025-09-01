package com.zengcode.eip.camel.ep3.requestreply;

import lombok.RequiredArgsConstructor;
import org.apache.camel.ProducerTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Service
@Profile({"ep3-reqreply","kafka"})
@RequiredArgsConstructor
public class PaymentsRpcGateway {

    private final ProducerTemplate producer;
    private final StringRedisTemplate redis;

    public String call(Map<String, Object> request, Duration timeout) {
        // สร้าง correlationId ตั้งแต่ฝั่ง gateway ให้ชัดเจน
        String corr = java.util.UUID.randomUUID().toString();

        Map<String,Object> headers = new HashMap<>();
        headers.put("correlationId", corr);
        headers.put("replyTo", "payments.rpc.replies");

        // ยิงเข้า route producer
        producer.sendBodyAndHeaders("direct:payments.rpc.request", request, headers);

        // รอผลที่ Redis list (reply listener จะ leftPush เข้ามา)
        String key = "rpc:reply:" + corr;
        String res = redis.opsForList().rightPop(key, timeout);

        if (res == null) {
            throw new RuntimeException("RPC timeout (corrId=" + corr + ")");
        }
        return res;
    }
}
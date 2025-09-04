package com.zengcode.eip.camel.ep4.resequencer.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderEvent {
    private String orderId;  // กลุ่มของเหตุการณ์ (correlation)
    private int step;        // ลำดับขั้น (sequence)
    private String type;     // ประเภทเหตุการณ์ (optional)
    private long ts;         // timestamp (optional)
    private String payload;  // ข้อมูลเสริม (optional)
}
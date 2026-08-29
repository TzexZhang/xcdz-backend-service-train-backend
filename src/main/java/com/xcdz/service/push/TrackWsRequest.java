package com.xcdz.service.push;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 软件单元:JAVA-WS-TRACK-WS_REQUEST-001
 * 功能:目标/批次推送客户端请求报文契约 v2（仅客户端 → 服务端方向；
 * 服务端响应见独立的 TrackWsMessage）
 * 协议:
 * ┌───────────── 客户端 → 服务端 ─────────────┐
 * │ SUBSCRIBE   订阅（targetName 为目标名称模糊关键字，空/缺省=全部目标；│
 * │             时间为绝对时间段，格式 yyyy-MM-dd HH:mm:ss，缺省=不限制） │
 * │ UNSUBSCRIBE 取消订阅（服务端仅移除订阅并记日志，不回执）             │
 * └──────────────────────────────────────────┘
 * 设计说明（v2 相对 v1 的变化）:双方向报文拆分为两个独立类，
 * 消除"共用一个结构靠 type 区分方向"的歧义；字段仅保留请求所需
 */
@Data
@Accessors(chain = true)
public class TrackWsRequest implements Serializable {

    /**
     * 订阅
     */
    public static final String TYPE_SUBSCRIBE = "SUBSCRIBE";
    /**
     * 取消订阅
     */
    public static final String TYPE_UNSUBSCRIBE = "UNSUBSCRIBE";

    //消息类型（取值为上述 TYPE_* 常量）
    private String type;

    //目标名称模糊关键字，LIKE 匹配 target.target_name；空/缺省 = 全部目标
    private String targetName;

    //时间范围下界 yyyy-MM-dd HH:mm:ss，空/缺省 = 不限制
    private String startTime;

    //时间范围上界 yyyy-MM-dd HH:mm:ss，空/缺省 = 不限制
    private String endTime;
}

package com.xcdz.service.push;

import com.xcdz.service.vo.TrackVO;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.List;

/**
 * 软件单元:JAVA-WS-TRACK-WS_MESSAGE-001
 * 功能:目标/批次推送业务报文契约（双方向共用一个报文结构，靠 type 区分）
 * 协议（端点 ws://host:9700/ws/data，均为 JSON 文本帧；PING/PONG/ERROR 由通用层承载）:
 * ┌───────────── 客户端 → 服务端 ─────────────┐
 * │ SUBSCRIBE   订阅（target 为目标名称模糊关键字，空串=全部目标；时间为绝对时间段，格式 yyyy-MM-dd HH:mm:ss，缺省=不限制） │
 * │ UNSUBSCRIBE 取消订阅（服务端仅移除订阅并记日志，不回执） │
 * ├───────────── 服务端 → 客户端 ─────────────┤
 * │ SUBSCRIBED      订阅确认（回显订阅条件）   │
 * │ INIT            订阅时存量快照（≤500 条）  │
 * │ DATA_ADD        增量推送新入库且匹配的数据 │
 * │ WINDOW_CLOSED   订阅窗口已结束（一次性；之后迟到补录数据若仍在窗口内仍会推 DATA_ADD） │
 * │ DATA_REMOVE     撤回已推送数据（预留：相对滑动窗口/数据删除场景，本期不发送） │
 * └──────────────────────────────────────────┘
 */
@Data
@Accessors(chain = true)
public class TrackWsMessage implements Serializable {
    //客户端 → 服务端
    /**
     * 订阅
     */
    public static final String TYPE_SUBSCRIBE = "SUBSCRIBE";
    /**
     * 取消订阅
     */
    public static final String TYPE_UNSUBSCRIBE = "UNSUBSCRIBE";

    //服务端 → 客户端
    /**
     * 订阅确认
     */
    public static final String TYPE_SUBSCRIBED = "SUBSCRIBED";
    /**
     * 订阅时存量快照
     */
    public static final String TYPE_INIT = "INIT";
    /**
     * 增量推送新入库且匹配的数据
     */
    public static final String TYPE_DATA_ADD = "DATA_ADD";
    /**
     * 订阅窗口已结束
     */
    public static final String TYPE_WINDOW_CLOSED = "WINDOW_CLOSED";
    /**
     * 撤回已推送数据（预留：相对滑动窗口/数据删除场景，本期不发送）
     */

    //消息类型（取值为上述 TYPE_* 常量）
    private String type;

    //目标名称模糊关键字（SUBSCRIBE 请求；SUBSCRIBED 回显），LIKE 匹配 target.target_name；空白 = 全部目标
    private String target;

    //时间范围下界 yyyy-MM-dd HH:mm:ss（SUBSCRIBE 请求 / SUBSCRIBED、WINDOW_CLOSED 回显），空 = 不限制
    private String startTime;

    //时间范围上界 yyyy-MM-dd HH:mm:ss，空 = 不限制
    private String endTime;

    //数据载体（INIT / DATA_ADD 使用）
    private List<TrackVO> data;

    //数据id集合（DATA_REMOVE 预留）
    private List<Long> ids;

    //附加说明（WINDOW_CLOSED 说明等）
    private String message;
}

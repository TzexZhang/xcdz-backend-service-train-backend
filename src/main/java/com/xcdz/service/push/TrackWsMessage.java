package com.xcdz.service.push;

import com.xcdz.service.vo.TrackVO;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.List;

/**
 * 软件单元:JAVA-WS-TRACK-WS_MESSAGE-001
 * 功能:目标/批次推送服务端报文契约 v2（CDC 方案配套协议，仅服务端 → 客户端方向；
 * 客户端请求见独立的 TrackWsRequest）
 * 协议（端点 ws://host:9700/ws/data，均为 JSON 文本帧）:
 * ┌───────────── 服务端 → 客户端 ─────────────┐
 * │ SUBSCRIBED  订阅确认（回显归一化后的订阅条件）           │
 * │ DATA        统一数据消息（phase 区分快照/实时两阶段）     │
 * │ READY       快照完整结束信号（此后 DATA 均为 LIVE）      │
 * │ CLOSED      订阅窗口已结束（一次性；迟到补录仍会来 DATA）│
 * │ ERROR       业务错误（连接不断，客户端可修正报文后重试）  │
 * └──────────────────────────────────────────┘
 * 设计说明（v2 相对 v1 的变化）:
 * 1. 合并:原 INIT 与 DATA_ADD 合并为 DATA + phase——快照与实时是同一条流的两个阶段，
 *    边界由独立信号 READY 表达，前端无需记忆"快照结束"的特例报文；
 * 2. 完整性契约:快照阶段由服务端流式分片下发全量符合条件的存量（无截断），
 *    服务端保证同一订阅内数据 id 不重复下发，前端直接 append 即可，不依赖任何 REST 对账；
 * 3. seq:会话级单调递增序号，前端本期可忽略；未来断线补齐/丢包检测只需
 *    客户端上报 lastSeq，协议零改动；
 * 4. DELETE 撤回不 overload 到 DATA（CDC 已能感知 track 删除，本期仅服务端日志，
 *    将来需要撤回推送时新增独立 type，如 REVOKE {ids}）
 */
@Data
@Accessors(chain = true)
public class TrackWsMessage implements Serializable {

    //服务端 → 客户端消息类型
    /**
     * 订阅确认（回显订阅条件）
     */
    public static final String TYPE_SUBSCRIBED = "SUBSCRIBED";
    /**
     * 统一数据消息（快照分片与实时增量共用）
     */
    public static final String TYPE_DATA = "DATA";
    /**
     * 快照完整结束信号
     */
    public static final String TYPE_READY = "READY";
    /**
     * 订阅窗口已结束
     */
    public static final String TYPE_CLOSED = "CLOSED";
    /**
     * 业务错误
     */
    public static final String TYPE_ERROR = "ERROR";

    //DATA 消息的阶段取值
    /**
     * 快照阶段（订阅时存量的流式分片）
     */
    public static final String PHASE_SNAPSHOT = "SNAPSHOT";
    /**
     * 实时阶段（READY 之后的所有 DATA）
     */
    public static final String PHASE_LIVE = "LIVE";

    //消息类型（取值为上述 TYPE_* 常量）
    private String type;

    //数据流阶段（仅 DATA 使用：SNAPSHOT / LIVE）
    private String phase;

    //会话级单调递增序号（仅 DATA 使用；从 1 起，快照与实时连续编号；前端可忽略）
    private Long seq;

    //快照分片序号（仅 DATA/SNAPSHOT 使用，从 1 起；调试与顺序确认用，无业务语义）
    private Integer part;

    //快照总条数（仅 READY 使用；0 表示订阅条件下无存量，同样视为快照完整）
    private Long count;

    //目标名称模糊关键字（SUBSCRIBED 回显归一化后的订阅条件）
    private String targetName;

    //时间范围下界 yyyy-MM-dd HH:mm:ss（SUBSCRIBED 回显 / CLOSED 携带窗口结束时间）
    private String startTime;

    //时间范围上界 yyyy-MM-dd HH:mm:ss，空 = 不限制
    private String endTime;

    //数据载体（DATA 使用）
    private List<TrackVO> data;

    //附加说明（ERROR 错误信息 / CLOSED 迟到补录说明）
    private String message;
}

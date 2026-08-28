package com.xcdz.service.websocket.protocol;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 软件单元:JAVA-WS-CORE-ENVELOPE-001
 * 功能:WebSocket 通用最小报文——协议级通知（PONG/ERROR）的载体
 * 通用层对上行报文的唯一约定:必须含字符串 type 字段，其余字段由业务自定义
 */
@Data
@Accessors(chain = true)
public class WsEnvelope implements Serializable {
    //客户端心跳探测（客户端 → 服务端，由通用路由层直接应答）
    public static final String TYPE_PING = "PING";

    //心跳应答（服务端 → 客户端）
    public static final String TYPE_PONG = "PONG";

    //错误信息（服务端 → 客户端，报文非法/未知类型等协议层错误）
    public static final String TYPE_ERROR = "ERROR";

    //消息类型（取值为各 TYPE_* 常量）
    private String type;

    //附加说明（ERROR 错误描述 / PONG 附言）
    private String message;

    //数据载体（通用通知预留，Object 类型保证通用层不感知业务结构）
    private Object data;
}

package com.xcdz.service.websocket.protocol;

import org.springframework.web.socket.WebSocketSession;

import java.util.Set;

/**
 * 软件单元:JAVA-WS-CORE-MESSAGE_PROCESSOR-001
 * 功能:WebSocket 业务消息处理器 SPI——通用路由层按 type 分发的业务扩展点
 * 使用方式:业务方实现本接口并注册为 Spring Bean，RoutingWebSocketHandler
 * 启动时自动收集容器内全部实现并按 supportTypes 构建路由表；报文体的解析由业务自理
 */
public interface WsMessageProcessor {

    /**
     * 声明本处理器负责的消息类型（type 字段取值，建议大写）
     */
    Set<String> supportTypes();

    /**
     * 处理一条上行消息（通用层已完成 JSON 合法性校验与 type 提取）
     *
     * @param session    来源连接
     * @param type       已归一化的消息类型（trim + 大写）
     * @param rawPayload 原始 JSON 文本（业务自行反序列化为自己的报文结构）
     */
    void process(WebSocketSession session, String type, String rawPayload);

    /**
     * 连接关闭时的业务清理钩子（如移除该连接的订阅状态）；默认空实现
     */
    default void onConnectionClosed(WebSocketSession session) {
    }
}

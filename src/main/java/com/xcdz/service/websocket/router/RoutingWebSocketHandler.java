package com.xcdz.service.websocket.router;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xcdz.service.websocket.protocol.WsEnvelope;
import com.xcdz.service.websocket.protocol.WsMessageProcessor;
import com.xcdz.service.websocket.session.WsSessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 软件单元:JAVA-WS-CORE-ROUTING_HANDLER-001
 * 功能:WebSocket 通用消息路由器——JSON 解析、按 type 分发到业务处理器、协议级应答
 * 通用性说明:本类不含任何业务概念，对上行报文的唯一约定是"含字符串 type 字段"；
 * 业务通过实现 WsMessageProcessor 接入（Spring 容器内全部实现自动注册），
 * PING→PONG 心跳与 ERROR 回执为协议内建行为
 * 分层契约:协议解析与路由在本层完成一次，报文体解析与业务规则全部下沉到 Processor
 * 注册方式:@Component + 构造注入（单构造器自动装配），WsSessionManager 与
 * 全部 WsMessageProcessor 实现由容器在启动期收集，路由表构建后运行期只读
 */
@Slf4j
@Component
public class RoutingWebSocketHandler extends TextWebSocketHandler {

    //type → 处理器 路由表（构造时由全部 WsMessageProcessor 实现构建，运行期只读）
    private final Map<String, WsMessageProcessor> routes = new HashMap<>();

    //全部业务处理器（连接关闭时逐一通知业务清理，每个处理器只通知一次）
    private final List<WsMessageProcessor> processors;

    //通用会话管理（连接登记与线程安全发送）
    private final WsSessionManager sessionManager;

    /**
     * @param sessionManager 通用会话管理器
     * @param processors     全部业务处理器（Spring 注入的 WsMessageProcessor 实现集合）
     */
    public RoutingWebSocketHandler(WsSessionManager sessionManager, List<WsMessageProcessor> processors) {
        this.sessionManager = sessionManager;
        this.processors = processors;
        for (WsMessageProcessor processor : processors) {
            for (String type : processor.supportTypes()) {
                routes.put(type, processor);
            }
        }
        log.info("WebSocket 路由表初始化，会话管理器: {}, 处理器: {}", sessionManager, processors);

    }

    /**
     * 连接建立:登记会话，等待客户端发送业务消息（如 SUBSCRIBE）
     */
    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessionManager.register(session);
        log.info("WebSocket 连接建立: {}", session.getId());
    }

    /**
     * 文本消息入口:解析 JSON → 提取 type → PING 直接应答 / 命中路由则委托业务处理器
     * 任何解析或参数错误都以 ERROR 消息回执，不中断连接
     */
    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        final String payload = message.getPayload();
        log.info("WebSocket 接收到文本消息: {}", payload);
        final JSONObject json;
        try {
            //fastjson 解析为通用对象，仅提取 type，不感知业务字段
            json = JSON.parseObject(payload);
        } catch (Exception e) {
            sendError(session, "报文不是合法 JSON: " + e.getMessage());
            return;
        }
        String type = json == null ? null : json.getString("type");
        if (type == null || type.trim().isEmpty()) {
            sendError(session, "缺少 type 字段");
            return;
        }
        String normalized = type.trim().toUpperCase();
        //协议内建心跳:PING → PONG
        if (WsEnvelope.TYPE_PING.equals(normalized)) {
            sessionManager.send(session, new WsEnvelope().setType(WsEnvelope.TYPE_PONG));
            return;
        }
        WsMessageProcessor processor = routes.get(normalized);
        if (processor == null) {
            sendError(session, "不支持的消息类型: " + type);
            return;
        }
        processor.process(session, normalized, payload);
    }

    /**
     * 连接关闭:移除会话（幂等）并逐处理器通知业务清理，会话表不残留
     */
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessionManager.remove(session.getId());
        for (WsMessageProcessor processor : processors) {
            processor.onConnectionClosed(session);
        }
        log.info("WebSocket 连接关闭: {}, status={}", session.getId(), status);
    }

    /**
     * 传输异常:记录日志并关闭连接（随后 afterConnectionClosed 完成清理）
     */
    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.warn("WebSocket 传输异常: {}, {}", session.getId(), exception.getMessage());
        sessionManager.closeQuietly(session);
    }

    /**
     * 向单个连接回执协议级错误（不中断连接，客户端可修正报文后重试）
     */
    private void sendError(WebSocketSession session, String message) {
        sessionManager.send(session, new WsEnvelope().setType(WsEnvelope.TYPE_ERROR).setMessage(message));
    }
}

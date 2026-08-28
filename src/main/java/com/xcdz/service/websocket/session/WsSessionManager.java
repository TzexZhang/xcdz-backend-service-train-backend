package com.xcdz.service.websocket.session;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 软件单元:JAVA-WS-CORE-SESSION_MANAGER-001
 * 功能:WebSocket 通用会话注册表——连接生命周期登记 + 线程安全的任意对象消息发送
 * 通用性说明:本类不含任何业务概念（订阅条件/推送语义等由业务侧自行管理），
 * 仅依赖 spring-websocket 与 fastjson，可整体复用到任意 WebSocket 端点
 * 线程安全说明:
 *   1. 会话表用 ConcurrentHashMap，连接建立/断开与业务线程并发安全；
 *   2. 单个 WebSocketSession 不允许并发写（sendMessage 非线程安全），
 *      所有发送均以 session 对象为锁串行化；
 *   3. 发送失败（IOException，如客户端已断开）时主动关闭并移除该会话，避免死会话累积
 */
@Slf4j
@Component
public class WsSessionManager {

    //全部在线连接：sessionId → session（连接建立即注册，断开即移除，与业务订阅状态无关）
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    /**
     * 登记连接（连接建立时调用），幂等
     */
    public void register(WebSocketSession session) {
        if (session != null) {
            sessions.put(session.getId(), session);
        }
    }

    /**
     * 移除连接（主动断开或连接关闭时调用），幂等
     */
    public void remove(String sessionId) {
        sessions.remove(sessionId);
    }

    /**
     * 按 id 查询在线连接
     *
     * @return 对应连接；不在线时返回 null
     */
    public WebSocketSession get(String sessionId) {
        return sessions.get(sessionId);
    }

    /**
     * 向指定连接发送消息（对 session 加锁串行发送，防止并发写异常）
     *
     * @param session 目标连接
     * @param msg     消息体（任意对象，fastjson 序列化为 JSON 文本帧）
     */
    public void send(WebSocketSession session, Object msg) {
        try {
            //synchronized(session)：WebSocketSession.sendMessage 非线程安全，同一连接的发送必须串行
            synchronized (session) {
                if (session.isOpen()) {
                    session.sendMessage(new TextMessage(JSON.toJSONString(msg)));
                }
            }
        } catch (IOException e) {
            //发送失败视为连接已失效：关闭并移除，防止死会话持续占用资源
            log.error("WebSocket 消息发送失败，移除会话 {}: {}", session.getId(), e.getMessage());
            remove(session.getId());
            closeQuietly(session);
        }
    }

    /**
     * 静默关闭连接（忽略关闭阶段的二次异常）
     */
    public void closeQuietly(WebSocketSession session) {
        try {
            session.close();
        } catch (IOException ignored) {
            //关闭失败无需处理，会话已被移除
        }
    }
}

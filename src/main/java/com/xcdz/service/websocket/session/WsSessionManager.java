package com.xcdz.service.websocket.session;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 软件单元:JAVA-WS-CORE-SESSION_MANAGER-001
 * 功能:WebSocket 通用会话注册表——连接生命周期登记 + 背压保护的线程安全消息发送
 * 通用性说明:本类不含任何业务概念（订阅条件/推送语义等由业务侧自行管理），
 * 仅依赖 spring-websocket 与 fastjson，可整体复用到任意 WebSocket 端点
 * <p>
 * 背压保护（企业级慢消费者防护，CDC 方案新增）:
 * 1. 登记时以 ConcurrentWebSocketSessionDecorator 包装原始连接——发送变为
 *    "消息入有界缓冲 + 后台串行 flush"，推送线程不再被慢客户端的 TCP 缓冲阻塞
 *    （原实现为同步阻塞写，慢消费者会卡住持 pushLock 的推送线程）；
 * 2. 溢出策略 = 断连:缓冲超限（bufferSizeLimit 字节）或单次发送超时
 *    （sendTimeLimit 毫秒）时自动关闭该连接并抛 SessionLimitExceededException——
 *    上层协议具备"重连即完整快照自愈"能力，慢消费者越早断开、越早恢复一致状态，
 *    优于静默丢消息；两个阈值经 push.ws.* 配置；
 * 3. 发送路由:send 以 sessionId 从注册表取包装后的连接发送——业务侧持有的
 *    原始 session 引用无需感知包装（两者 getId 一致）
 * 线程安全说明:
 *   1. 会话表用 ConcurrentHashMap，连接建立/断开与业务线程并发安全；
 *   2. 单连接的并发写由 decorator 内部串行化（取代原 synchronized(session)）；
 *   3. 发送失败（IOException，如客户端已断开）时主动关闭并移除该会话，避免死会话累积
 */
@Slf4j
@Component
public class WsSessionManager {

    //单次发送超时毫秒数（超时视为慢消费者，断连自愈）
    @Value("${push.ws.send-time-limit-ms:5000}")
    private long sendTimeLimit;

    //单连接发送缓冲字节上限（超限视为慢消费者，断连自愈；默认 512KB）
    @Value("${push.ws.buffer-size-limit-bytes:524288}")
    private int bufferSizeLimit;

    //全部在线连接：sessionId → 包装后的会话（连接建立即注册，断开即移除，与业务订阅状态无关）
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    /**
     * 登记连接（连接建立时调用，幂等）——以背压包装器注册，
     * 此后所有发送经有界缓冲异步化，慢消费者不再阻塞推送线程
     */
    public void register(WebSocketSession session) {
        if (session != null) {
            sessions.put(session.getId(),
                    new ConcurrentWebSocketSessionDecorator(session, (int) sendTimeLimit, bufferSizeLimit));
        }
    }

    /**
     * 移除连接（主动断开或连接关闭时调用），幂等
     */
    public void remove(String sessionId) {
        sessions.remove(sessionId);
    }

    /**
     * 按 id 查询在线连接（返回注册时的包装会话；业务侧一般直接持有原始引用，仅个别场景需要）
     *
     * @return 对应连接；不在线时返回 null
     */
    public WebSocketSession get(String sessionId) {
        return sessions.get(sessionId);
    }

    /**
     * 向指定连接发送消息（经注册表的包装会话发送，享受缓冲与超时保护；
     * 传入原始 session 引用同样按 id 路由到包装会话）
     *
     * @param session 目标连接（原始或包装引用均可）
     * @param msg     消息体（任意对象，fastjson 序列化为 JSON 文本帧）
     */
    public void send(WebSocketSession session, Object msg) {
        if (session == null) {
            return;
        }
        WebSocketSession delegate = sessions.get(session.getId());
        if (delegate == null) {
            //未注册（连接尚未建立完成或已被移除），静默丢弃
            return;
        }
        try {
            if (delegate.isOpen()) {
                delegate.sendMessage(new TextMessage(JSON.toJSONString(msg)));
            }
        } catch (Exception e) {
            //发送失败视为连接已失效（含慢消费者溢出 SessionLimitExceededException）：
            //关闭并移除，防止死会话持续占用资源；客户端重连即走完整快照自愈
            log.error("WebSocket 消息发送失败，移除会话 {}: {}", session.getId(), e.getMessage());
            remove(session.getId());
            closeQuietly(delegate);
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

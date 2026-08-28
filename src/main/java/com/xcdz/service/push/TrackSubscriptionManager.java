package com.xcdz.service.push;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 软件单元:JAVA-WS-TRACK-SUBSCRIPTION_MANAGER-001
 * 功能:目标/批次推送订阅表——维护"每个连接的业务订阅条件与推送水位"
 * 分层契约:本类只管业务订阅状态，不含连接登记与消息发送（两者由通用层 websocket/WsSessionManager 承担），
 * 推送侧通过 Subscription.session 借助通用会话管理器发送
 * 线程安全说明:订阅表用 ConcurrentHashMap，订阅/退订/断开与推送轮次并发安全
 */
@Component
public class TrackSubscriptionManager {

    /**
     * 单个连接的订阅状态（一个连接同一时刻只持有一份订阅，重复 SUBSCRIBE = 重置订阅重发 INIT）
     */
    public static class Subscription {
        //对应 WebSocket 连接
        private final WebSocketSession session;
        //目标名称模糊关键字；null 表示全部目标（SUBSCRIBE 未传或传空白），
        //过滤语义与快照查询的 LIKE 完全一致（contains 实现）
        private final String nameFilter;
        //采集时间下界（含）；null 表示不限制
        private final LocalDateTime startTime;
        //采集时间上界（含）；null 表示不限制
        private final LocalDateTime endTime;
        //该会话已考虑（无论是否推送）的最大数据 id，用于增量批次去重，只增不减
        private long lastPushedId;
        //窗口关闭通知是否已发送过（WINDOW_CLOSED 只发一次）
        private boolean windowClosedNotified;

        Subscription(WebSocketSession session, String nameFilter,
                     LocalDateTime startTime, LocalDateTime endTime, long lastPushedId) {
            this.session = session;
            this.nameFilter = nameFilter;
            this.startTime = startTime;
            this.endTime = endTime;
            this.lastPushedId = lastPushedId;
        }

        public WebSocketSession getSession() {
            return session;
        }

        public String getNameFilter() {
            return nameFilter;
        }

        public LocalDateTime getStartTime() {
            return startTime;
        }

        public LocalDateTime getEndTime() {
            return endTime;
        }

        public long getLastPushedId() {
            return lastPushedId;
        }

        public void setLastPushedId(long lastPushedId) {
            this.lastPushedId = lastPushedId;
        }

        public boolean isWindowClosedNotified() {
            return windowClosedNotified;
        }

        public void setWindowClosedNotified(boolean windowClosedNotified) {
            this.windowClosedNotified = windowClosedNotified;
        }
    }

    //订阅表：sessionId → 订阅状态（仅保存已 SUBSCRIBE 的会话；连上但未订阅的连接不入表）
    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();

    /**
     * 登记订阅（同连接重复订阅时直接覆盖旧订阅 = 重置）
     *
     * @param sub 订阅状态（含初始水位）
     */
    public void subscribe(Subscription sub) {
        subscriptions.put(sub.getSession().getId(), sub);
    }

    /**
     * 移除会话订阅（主动 UNSUBSCRIBE 或连接断开时调用），幂等
     */
    public void remove(String sessionId) {
        subscriptions.remove(sessionId);
    }

    /**
     * 当前是否存在订阅会话（推送轮次为空时跳过，避免无谓查库）
     */
    public boolean isEmpty() {
        return subscriptions.isEmpty();
    }

    /**
     * 全部订阅快照（遍历期间结构变更不影响已取出的快照）
     */
    public Collection<Subscription> all() {
        return Collections.unmodifiableCollection(subscriptions.values());
    }

    /**
     * 构建订阅状态（target 空白/null → null 关键字 = 全部目标，入口层已归一化）
     */
    public static Subscription buildSubscription(WebSocketSession session, String target,
                                                 LocalDateTime startTime, LocalDateTime endTime, long lastPushedId) {
        return new Subscription(session, target, startTime, endTime, lastPushedId);
    }
}

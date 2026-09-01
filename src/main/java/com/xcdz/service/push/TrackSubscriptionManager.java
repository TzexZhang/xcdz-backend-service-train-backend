package com.xcdz.service.push;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 软件单元:JAVA-WS-TRACK-SUBSCRIPTION_MANAGER-001
 * 功能:目标/批次推送订阅表——维护"每个连接的业务订阅条件、推送水位与事件路由索引"
 * <p>
 * 核心机制（CDC 方案的事件路由）:
 * 1. 双结构索引:主表（sessionId → Subscription）承担生命周期管理；
 *    倒排路由索引（targetId → 订阅集合）+ wildcard 全域集合承担事件分发时的候选订阅查找，
 *    将旧"每事件扫描全部订阅"降为"每事件一次索引命中"；
 * 2. 路由展开时机:订阅登记时由调用方（TrackPushTask，持 pushLock）依据
 *    TargetNameCache 将名称关键字反查为 targetId 集合后传入——本类不感知缓存，
 *    保持纯状态管理职责；target 表改名/新增时由 TargetNameCache 驱动 rebind 重算分组；
 * 3. 水位去重语义不变:Subscription.lastPushedId 为该会话"已考虑（无论是否推送）"
 *    的最大数据 id，由分发轮末统一推进，消化 Kafka at-least-once 的重复消费；
 * 4. seq:会话级 DATA 消息单调递增序号（v2 协议），调用均在 pushLock 内，
 *    普通 long 自增即线程安全
 * 分层契约:本类只管业务订阅状态，不含连接登记与消息发送（两者由通用层
 * websocket/WsSessionManager 承担）
 * 线程安全说明:订阅/退订/断开与推送轮次并发——subscribe/rebind 在 pushLock 内
 * 串行调用；remove 可能来自 WS 关闭回调（无锁），结构操作全部依赖并发容器；
 * target 改名重算（rebind）与事件分发（candidates 读取）并发下的瞬时新旧分组
 * 交替可接受（目标表变更频率极低，最终一致）
 */
@Component
public class TrackSubscriptionManager {

    /**
     * 单个连接的订阅状态（一个连接同一时刻只持有一份订阅，重复 SUBSCRIBE = 重置订阅重发快照）
     */
    public static class Subscription {
        //对应 WebSocket 连接
        @Getter
        private final WebSocketSession session;
        //目标名称模糊关键字；null 表示全部目标（SUBSCRIBE 未传或传空白），
        //过滤语义与快照查询的 LIKE 完全一致（contains 实现）
        @Getter
        private final String nameFilter;
        //采集时间下界（含）；null 表示不限制
        @Getter
        private final LocalDateTime startTime;
        //采集时间上界（含）；null 表示不限制
        @Getter
        private final LocalDateTime endTime;
        //该会话已考虑（无论是否推送）的最大数据 id（String 雪花id，字典序=数值序），
        //用于增量批次去重，只增不减；null = 尚无推进（全部视为新数据）
        @Setter
        @Getter
        private String lastPushedId;
        //窗口关闭通知是否已发送过（CLOSED 只发一次）
        @Setter
        @Getter
        private boolean windowClosedNotified;
        //会话级 DATA 消息单调递增序号（v2 协议；调用均在 pushLock 内，无需原子类）
        private long seq = 0L;
        //当前已展开注册的路由分组（targetId 集合）；由本 Manager 维护，
        //用于退订/重算时的精确反向清理；null 表示 wildcard 全域订阅
        private Set<String> routedTargetIds;

        Subscription(WebSocketSession session, String nameFilter,
                     LocalDateTime startTime, LocalDateTime endTime, String lastPushedId) {
            this.session = session;
            this.nameFilter = nameFilter;
            this.startTime = startTime;
            this.endTime = endTime;
            this.lastPushedId = lastPushedId;
        }

        /**
         * 生成下一个会话级序号（v2 协议 DATA 消息的 seq 字段，从 1 起连续编号）
         */
        public long nextSeq() {
            return ++seq;
        }
    }

    //订阅主表：sessionId → 订阅状态（仅保存已 SUBSCRIBE 的会话；连上但未订阅的连接不入表）
    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();

    //倒排路由索引：targetId → 订阅集合（仅关键字订阅；wildcard 订阅不在此注册）
    private final Map<String, Set<Subscription>> routeIndex = new ConcurrentHashMap<>();

    //全域订阅集合（nameFilter = null 的订阅，任意 targetId 的事件都路由到它们）
    private final Set<Subscription> wildcard = ConcurrentHashMap.newKeySet();

    /**
     * 登记订阅并展开路由分组（同连接重复订阅时直接覆盖旧订阅 = 重置；
     * 覆盖前先精确清理旧订阅的路由注册，防止残留）
     *
     * @param sub       订阅状态（含初始水位）
     * @param targetIds 该订阅关键字当前命中的 targetId 集合（依据 TargetNameCache 反查）；
     *                  null 表示 wildcard 全域订阅（关键字为空）
     */
    public void subscribe(Subscription sub, Set<String> targetIds) {
        //先清理同连接可能存在的旧订阅（重订阅场景）
        remove(sub.getSession().getId());
        subscriptions.put(sub.getSession().getId(), sub);
        registerRoutes(sub, targetIds);
    }

    /**
     * 移除会话订阅（主动 UNSUBSCRIBE 或连接断开时调用），幂等；
     * 依据 routedTargetIds 精确反向清理路由分组，避免全索引扫描
     */
    public void remove(String sessionId) {
        Subscription old = subscriptions.remove(sessionId);
        if (old == null) {
            return;
        }
        unregisterRoutes(old);
    }

    /**
     * 当前是否存在订阅会话
     */
    public boolean isEmpty() {
        return subscriptions.isEmpty();
    }

    /**
     * 全部订阅快照（遍历期间结构变更不影响已取出的快照；窗口检测/水位推进等遍历场景使用）
     */
    public Collection<Subscription> all() {
        return Collections.unmodifiableCollection(subscriptions.values());
    }

    /**
     * 事件路由:查询指定目标的事件应分发给的候选订阅集合
     * （wildcard 全域订阅 ∪ 该 targetId 分组下的关键字订阅）
     *
     * @return 候选订阅快照集合（含未带该目标关键字的 wildcard 订阅；
     *         调用方仍需做时间窗过滤与水位去重）
     */
    public Set<Subscription> candidates(String targetId) {
        Set<Subscription> group = routeIndex.get(targetId);
        if (group == null || group.isEmpty()) {
            return Set.copyOf(wildcard);
        }
        Set<Subscription> merged = new HashSet<>(wildcard.size() + group.size());
        merged.addAll(wildcard);
        merged.addAll(group);
        return merged;
    }

    /**
     * 重算订阅的路由分组（target 表改名/新增/删除后由 TargetNameCache 驱动；
     * 仅对关键字订阅有意义，wildcard 订阅无需重算）
     *
     * @param sub       订阅状态
     * @param newIds    关键字当前最新命中的 targetId 集合
     */
    public void rebind(Subscription sub, Set<String> newIds) {
        if (sub.getNameFilter() == null) {
            return; //wildcard 订阅与目标名称无关
        }
        unregisterRoutes(sub);
        registerRoutes(sub, newIds);
    }

    /**
     * 构建订阅状态（targetName 空白/null → null 关键字 = 全部目标，入口层已归一化；
     * lastPushedId 为初始水位，null = 尚无推进）
     */
    public static Subscription buildSubscription(WebSocketSession session, String targetName,
                                                 LocalDateTime startTime, LocalDateTime endTime, String lastPushedId) {
        return new Subscription(session, targetName, startTime, endTime, lastPushedId);
    }

    /**
     * 注册路由分组:wildcard 进全域集合；关键字订阅展开进各 targetId 分组，
     * 并记录 routedTargetIds 供反向清理
     */
    private void registerRoutes(Subscription sub, Set<String> targetIds) {
        if (targetIds == null) {
            sub.routedTargetIds = null;
            wildcard.add(sub);
            return;
        }
        sub.routedTargetIds = new HashSet<>(targetIds);
        for (String targetId : targetIds) {
            /**
             * ConcurrentHashMap.newKeySet()：返回一个基于 ConcurrentHashMap 实现的线程安全 Set
             */
            routeIndex.computeIfAbsent(targetId, k -> ConcurrentHashMap.newKeySet()).add(sub);
        }
    }

    /**
     * 注销路由分组:依据 routedTargetIds 精确清理（wildcard 从全域集合移除）
     */
    private void unregisterRoutes(Subscription sub) {
        if (sub.routedTargetIds == null) {
            wildcard.remove(sub);
            return;
        }
        for (String targetId : sub.routedTargetIds) {
            Set<Subscription> group = routeIndex.get(targetId);
            if (group != null) {
                group.remove(sub);
                //分组清空后移除键，防止路由索引随目标增删无限膨胀
                if (group.isEmpty()) {
                    routeIndex.remove(targetId, group);
                }
            }
        }
        sub.routedTargetIds = null;
    }
}

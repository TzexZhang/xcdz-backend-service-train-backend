package com.xcdz.service.push;

import com.xcdz.service.dto.TrackDetailDTO;
import com.xcdz.service.dto.TrackSubscribeDTO;
import com.xcdz.service.push.cdc.TargetNameCache;
import com.xcdz.service.service.TrackService;
import com.xcdz.service.utils.TrackConvert;
import com.xcdz.service.websocket.session.WsSessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 软件单元:JAVA-WS-TRACK-PUSH_TASK-001
 * 功能:目标/批次推送任务——订阅时的流式完整快照下发 + CDC 增量事件分发
 * <p>
 * 核心机制（ADR，CDC 方案）:
 * 1. 感知方式为 binlog CDC（Debezium→Kafka→TrackCdcConsumer→onCdcEvents），
 *    与写入方完全解耦（业务代码写入、模拟器写入、外部 SQL 手动写入一视同仁），
 *    毫秒级感知；
 * 2. 快照完整性契约:订阅时按订阅条件游标分页流式下发全量存量（无截断），
 *    DATA/SNAPSHOT 分片 + READY 结束信号——服务端保证同一订阅内数据 id 唯一下发，
 *    前端直接 append 即可，无需任何去重；
 * 3. 服务端无重保障（四层合围）:快照由主键唯一 + 严格递增游标天然无重；
 *    同批 CDC 重复 id 由分发前按 id 收敛消除（水位轮末才推进，覆盖不了同批）；
 *    cdcSeenMaxId 为 CDC 通道已推进的最大数据 id（订阅快照的基准底线，
 *    防 Kafka rebalance 重消费旧事件跨轮重推）；会话 lastPushedId 为每订阅
 *    私有去重基准，消化快照/增量交叉与跨轮重复消费；
 *    发送层无重试（失败即断连，重连重订阅 = 完整快照重新同步）
 *    id 均为 String 雪花id（19 位定长数字字符串，字典序与数值序一致），null = 尚无推进;
 * 4. pushLock 互斥锁:订阅流程（读基准→流式快照→登记）与 CDC 分发轮次
 *    （路由→过滤→发送→推进水位）串行执行，消除"快照与增量批次交叉"导致的
 *    重复推送/漏推；
 * 5. 事件路由分发:事件按 targetId 经订阅管理器的倒排索引命中候选订阅
 *    （wildcard 全域 ∪ 关键字分组），候选内再做时间窗过滤与水位去重；
 *    轮末统一推进被触达订阅的水位（路由排除的事件 = 已考虑，只增不减）；
 * 6. 已过窗口结束时间（now > endTime）的会话推送一次 CLOSED 后不再重复通知，
 *    但迟到补录数据若其 collect_time 仍在窗口内，仍会正常 DATA/LIVE
 *    （按数据自身时间过滤，而非数据到达时间——契约约定）；
 *    窗口到期与事件到达无关，由每秒轻量定时检查兜底通知（纯内存，无查库）；
 * 7. 与 WebSocket 通用层解耦:本类属业务编排层，只依赖业务订阅表
 *    （TrackSubscriptionManager）与通用会话管理器（WsSessionManager）的发送能力
 * 分层契约:数据访问经 TrackService，出参转换经 TrackConvert
 */
@Slf4j
@Component
public class TrackPushTask {

    //流式快照单页条数（游标分页页大小；仅控制单次查库与单帧报文体量，不构成截断——查满一页继续翻页，直到查不满一页为止，存量始终完整下发）
    public static final int SNAPSHOT_PAGE_SIZE = 500;

    @Autowired
    private TrackService trackService;

    @Autowired
    private TrackSubscriptionManager subscriptionManager;

    @Autowired
    private TargetNameCache targetCache;

    @Autowired
    private WsSessionManager sessionManager;

    //CDC 通道已分发（或已跳过）的最大数据 id（String 雪花id，字典序=数值序）；
    //null 表示尚无 CDC 事件推进。取代旧轮询方案 globalWatermark 的角色，
    //由 CDC 消费线程在锁内推进，作为新订阅快照的水位基准底线
    private volatile String cdcSeenMaxId = null;

    //订阅流程与 CDC 分发轮次的互斥锁（见类注释第 4 点）
    private final Object pushLock = new Object();

    /**
     * 订阅:订阅确认 → 流式完整快照（DATA/SNAPSHOT 分片）→ 登记 → READY
     *
     * @param session WebSocket 连接
     * @param dto     订阅条件（targetName 已由入口层归一化:空=全部目标；时间已解析）
     */
    public void subscribe(WebSocketSession session, TrackSubscribeDTO dto) {
        synchronized (pushLock) {
            //快照基准:CDC 通道已推进位置——空快照时的水位底线（防重消费旧事件重推）；
            //快照期间到达的增量由随后的 CDC 轮次覆盖（锁保证不交叉）
            String base = cdcSeenMaxId;
            TrackSubscriptionManager.Subscription sub = TrackSubscriptionManager.buildSubscription(
                    session, dto.getTargetName(), dto.getStartTime(), dto.getEndTime(), base);

            //先发订阅确认（回显归一化条件），前端收到后重置本地列表
            sessionManager.send(sub.getSession(), new TrackWsMessage()
                    .setType(TrackWsMessage.TYPE_SUBSCRIBED)
                    .setTargetName(dto.getTargetName())
                    .setStartTime(dto.getStartTime() == null ? null : TrackConvert.DATE_TIME_FORMATTER.format(dto.getStartTime()))
                    .setEndTime(dto.getEndTime() == null ? null : TrackConvert.DATE_TIME_FORMATTER.format(dto.getEndTime())));

            //流式完整快照:游标分页直到查不满一页（无截断、无对账）；
            //单页查库期间的新增数据由随后的 CDC 增量轮次覆盖，水位去重消化交叉重复
            String cursor = null;
            long total = 0L;
            int part = 0;
            while (true) {
                List<TrackDetailDTO> page = trackService.findByCondition(
                        dto.getTargetName(), dto.getStartTime(), dto.getEndTime(), cursor, SNAPSHOT_PAGE_SIZE);
                if (page.isEmpty()) {
                    break;
                }
                part++;
                total += page.size();
                for (TrackDetailDTO row : page) {
                    cursor = maxId(cursor, row.getId());
                }
                sessionManager.send(sub.getSession(), new TrackWsMessage()
                        .setType(TrackWsMessage.TYPE_DATA)
                        .setPhase(TrackWsMessage.PHASE_SNAPSHOT)
                        .setPart(part)
                        .setSeq(sub.nextSeq())
                        .setData(TrackConvert.toVOList(page)));
                if (page.size() < SNAPSHOT_PAGE_SIZE) {
                    break;
                }
            }
            //快照内最大 id 抬高会话水位（快照可能包含基准之后新插入的行），
            //随后登记订阅并展开路由分组（关键字经维度缓存反查 targetId 集合）
            sub.setLastPushedId(maxId(base, cursor));
            subscriptionManager.subscribe(sub, targetCache.matchingTargetIds(dto.getTargetName()));

            //快照完整结束信号（count=0 同样视为完整快照；此后 DATA 均为 LIVE）
            sessionManager.send(sub.getSession(), new TrackWsMessage()
                    .setType(TrackWsMessage.TYPE_READY)
                    .setCount(total));
            log.info("会话 {} 订阅成功: targetName={}, time=[{}, {}], 快照 {} 条/{} 片, 会话水位 {}",
                    session.getId(), dto.getTargetName(), dto.getStartTime(), dto.getEndTime(), total, part, sub.getLastPushedId());
        }
    }

    /**
     * 取消订阅（幂等；主动退订与连接关闭两个入口均调用此处。
     * 结构操作由并发容器保证安全，无需持锁）
     */
    public void unsubscribe(WebSocketSession session) {
        subscriptionManager.remove(session.getId());
        log.info("会话 {} 订阅已移除", session.getId());
    }

    /**
     * CDC 增量分发轮次:TrackCdcConsumer 解析出的一批事件 → 批内按 id 去重
     * → 按 targetId 路由命中候选订阅 → 时间窗过滤 + 水位去重 → DATA/LIVE → 轮末统一推进水位与通道基准
     *
     * @param events 单批 CDC 事件（DTO 已补齐 targetName）
     */
    public void onCdcEvents(List<TrackDetailDTO> events) {
        synchronized (pushLock) {
            String eventsMaxId = null;
            //批内去重:Kafka at-least-once 的重复消费可能使同一 poll 批携带重复 id
            //（同批重复跨不过水位——水位轮末才推进），分发前必须按 id 收敛；
            //同 id 后写覆盖前写（幂等取最新值），顺序保持首次出现位置
            Map<String, TrackDetailDTO> distinct = new LinkedHashMap<>(events.size());
            for (TrackDetailDTO event : events) {
                distinct.put(event.getId(), event);
            }
            //按 targetId 分组（路由查找的最小粒度）
            Map<String, List<TrackDetailDTO>> byTarget = new HashMap<>();
            for (TrackDetailDTO event : distinct.values()) {
                eventsMaxId = maxId(eventsMaxId, event.getId());
                byTarget.computeIfAbsent(event.getTargetId(), k -> new ArrayList<>()).add(event);
            }

            //路由分发:候选订阅（wildcard ∪ 关键字分组）内做时间窗过滤与水位去重
            Map<TrackSubscriptionManager.Subscription, List<TrackDetailDTO>> matchedBySub = new LinkedHashMap<>();
            Set<TrackSubscriptionManager.Subscription> touched = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Map.Entry<String, List<TrackDetailDTO>> group : byTarget.entrySet()) {
                for (TrackSubscriptionManager.Subscription sub : subscriptionManager.candidates(group.getKey())) {
                    //单会话异常不影响其他会话的推送
                    try {
                        touched.add(sub);
                        List<TrackDetailDTO> matched = filterMatched(sub, group.getValue());
                        if (!matched.isEmpty()) {
                            matchedBySub.computeIfAbsent(sub, k -> new ArrayList<>()).addAll(matched);
                        }
                    } catch (Exception e) {
                        log.error("会话 {} 增量分发异常: {}", sub.getSession().getId(), e.getMessage(), e);
                    }
                }
            }
            for (Map.Entry<TrackSubscriptionManager.Subscription, List<TrackDetailDTO>> entry : matchedBySub.entrySet()) {
                TrackSubscriptionManager.Subscription sub = entry.getKey();
                sessionManager.send(sub.getSession(), new TrackWsMessage()
                        .setType(TrackWsMessage.TYPE_DATA)
                        .setPhase(TrackWsMessage.PHASE_LIVE)
                        .setSeq(sub.nextSeq())
                        .setData(TrackConvert.toVOList(entry.getValue())));
            }

            //轮末统一推进:被触达订阅的水位推进到本批最大 id（路由排除/过滤不匹配 = 已考虑，
            //避免历史批次被反复过滤）；通道基准只增不减
            for (TrackSubscriptionManager.Subscription sub : touched) {
                sub.setLastPushedId(maxId(sub.getLastPushedId(), eventsMaxId));
            }
            cdcSeenMaxId = maxId(cdcSeenMaxId, eventsMaxId);
        }
    }

    /**
     * 窗口到期检查（每秒轻量兜底）:
     * CDC 感知是事件驱动的，窗口关闭与"是否还有新事件"无关——
     * 无事件流时也必须按时通知 CLOSED，故保留纯内存定时检查（fixedDelay 无轮次重叠；
     * 与模拟器共用 Spring 默认单线程调度器，任务间串行执行）
     */
    @Scheduled(fixedDelay = 1000)
    public void windowCheckTick() {
        if (subscriptionManager.isEmpty()) {
            return;
        }
        synchronized (pushLock) {
            for (TrackSubscriptionManager.Subscription sub : subscriptionManager.all()) {
                checkWindowClosed(sub);
            }
        }
    }

    /**
     * 窗口关闭检测:now > endTime 时一次性推送 CLOSED
     * 注意:通知后不停止增量过滤——迟到补录数据（collect_time 仍在窗口内）继续推 DATA/LIVE
     */
    private void checkWindowClosed(TrackSubscriptionManager.Subscription sub) {
        if (!sub.isWindowClosedNotified() && sub.getEndTime() != null
                && LocalDateTime.now().isAfter(sub.getEndTime())) {
            sub.setWindowClosedNotified(true);
            sessionManager.send(sub.getSession(), new TrackWsMessage()
                    .setType(TrackWsMessage.TYPE_CLOSED)
                    .setEndTime(TrackConvert.DATE_TIME_FORMATTER.format(sub.getEndTime()))
                    .setMessage("订阅时间窗口已结束，不再有新的实时数据；迟到补录数据若仍在窗口内会继续推送"));
            log.info("会话 {} 订阅窗口已关闭: endTime={}", sub.getSession().getId(), sub.getEndTime());
        }
    }

    /**
     * 候选订阅的事件精过滤:水位去重（id > lastPushedId）+ 时间窗口
     * （闭区间，按数据自身采集时间，与推送/到达时刻无关——迟到数据语义）；
     * 目标匹配已由路由索引完成（wildcard 天然命中，关键字订阅路由即命中）
     */
    private List<TrackDetailDTO> filterMatched(TrackSubscriptionManager.Subscription sub,
                                               List<TrackDetailDTO> rows) {
        List<TrackDetailDTO> matched = new ArrayList<>();
        for (TrackDetailDTO row : rows) {
            //水位去重:null 水位（尚无推进）视为全部新数据；
            //19 位定长数字字符串字典序与数值序一致，直接 compareTo 比较
            if (sub.getLastPushedId() != null && row.getId().compareTo(sub.getLastPushedId()) <= 0) {
                continue;
            }
            if (row.getCollectTime() == null) {
                continue;
            }
            boolean afterStart = sub.getStartTime() == null || !row.getCollectTime().isBefore(sub.getStartTime());
            boolean beforeEnd = sub.getEndTime() == null || !row.getCollectTime().isAfter(sub.getEndTime());
            if (afterStart && beforeEnd) {
                matched.add(row);
            }
        }
        return matched;
    }

    /**
     * 雪花 id 取较大者（19 位定长数字字符串，字典序与数值序一致，直接字符串比较）；
     * 任一为 null（尚无水位推进）返回另一个，双 null 返回 null
     */
    private static String maxId(String a, String b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) >= 0 ? a : b;
    }
}

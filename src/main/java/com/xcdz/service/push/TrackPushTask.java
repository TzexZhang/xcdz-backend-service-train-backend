package com.xcdz.service.push;

import com.xcdz.service.dto.TrackDetailDTO;
import com.xcdz.service.dto.TrackSubscribeDTO;
import com.xcdz.service.service.TrackService;
import com.xcdz.service.utils.TrackConvert;
import com.xcdz.service.websocket.session.WsSessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.util.List;

/**
 * 软件单元:JAVA-WS-TRACK-PUSH_TASK-001
 * 功能:目标/批次增量推送任务——感知"第三方"新入库数据并按各会话订阅条件推送
 * <p>
 * 核心机制（ADR）:
 * 1. 感知方式为 DB 水位轮询:每秒按 track 自增 id 水位查增量，
 *    与写入方完全解耦（业务代码写入、模拟器写入、外部 SQL 手动写入一视同仁）；
 *    真实生产可平滑替换为 binlog CDC（Canal/Debezium→Kafka），仅换感知层，推送协议不变；
 * 2. 全局水位 + 会话级水位两级去重:全局水位决定"查什么"，会话水位决定"该会话是否已考虑过"，
 *    避免每会话独立查库；
 * 3. pushLock 互斥锁:订阅流程（读全局水位→查 INIT 快照→登记会话）与推送轮次（查增量→逐会话分发）
 *    串行执行，消除"订阅瞬间快照与增量批次交叉"导致的重复推送/漏推；
 * 4. 已过窗口结束时间（now > endTime）的会话推送一次 WINDOW_CLOSED 后不再重复通知，
 *    但迟到补录数据若其 collect_time 仍在窗口内，仍会正常 DATA_ADD（按数据自身时间过滤，
 *    而非数据到达时间——契约约定）；
 * 5. 与 WebSocket 通用层解耦:本类属业务编排层，只依赖业务订阅表（TrackSubscriptionManager）
 *    与通用会话管理器（websocket/session/WsSessionManager）的发送能力，消息路由与连接管理均在通用层
 * 分层契约:数据访问经 TrackService，出参转换经 TrackConvert
 */
@Slf4j
@Component
public class TrackPushTask {

    //INIT 快照最大条数（防止订阅宽条件时全量轰炸前端；超出部分由 REST 对账接口分页查询）
    public static final int INIT_LIMIT = 500;

    //单轮增量批量上限（写入洪峰时单轮最多处理 1000 条，剩余下一轮继续，水位保证不丢）
    public static final int BATCH_LIMIT = 1000;

    @Autowired
    private TrackService trackService;

    @Autowired
    private TrackSubscriptionManager subscriptionManager;

    @Autowired
    private WsSessionManager sessionManager;

    //全局水位:已扫描处理过的最大数据 id；只增不减。0 配合 watermarkInitialized 表示尚未初始化
    private volatile long globalWatermark = 0L;

    //水位是否已用表内 max(id) 初始化过（初始化 = 跳过历史存量，存量只经 INIT 快照下发）
    private volatile boolean watermarkInitialized = false;

    //订阅流程与推送轮次的互斥锁（见类注释第 3 点）
    private final Object pushLock = new Object();

    /**
     * 订阅:存量快照 + 登记 + SUBSCRIBED/INIT 下发
     *
     * @param session WebSocket 连接
     * @param dto     订阅条件（target 已由入口层归一化:空=全部目标；时间已解析）
     */
    public void subscribe(WebSocketSession session, TrackSubscribeDTO dto) {
        synchronized (pushLock) {
            ensureWatermark();
            //先取当前全局水位作为基准（快照查询期间的新增数据由随后的增量轮次覆盖）
            long baseWatermark = globalWatermark;
            //存量快照:只下发符合订阅条件的记录
            List<TrackDetailDTO> snapshot = trackService.findByCondition(
                    dto.getTarget(), dto.getStartTime(), dto.getEndTime(), INIT_LIMIT);
            //会话初始水位取"全局水位"与"快照内最大id"的较大者:
            //快照可能包含水位之后新插入的行，抬高会话水位避免增量轮次重复下发这些行
            long snapshotMaxId = snapshot.stream().mapToLong(TrackDetailDTO::getId).max().orElse(baseWatermark);

            TrackSubscriptionManager.Subscription sub = TrackSubscriptionManager.buildSubscription(
                    session, dto.getTarget(), dto.getStartTime(), dto.getEndTime(), Math.max(baseWatermark, snapshotMaxId));
            subscriptionManager.subscribe(sub);

            //先发订阅确认（回显条件），再发存量快照，前端按序处理
            sessionManager.send(sub.getSession(), new TrackWsMessage()
                    .setType(TrackWsMessage.TYPE_SUBSCRIBED)
                    .setTarget(dto.getTarget())
                    .setStartTime(dto.getStartTime() == null ? null : TrackConvert.DATE_TIME_FORMATTER.format(dto.getStartTime()))
                    .setEndTime(dto.getEndTime() == null ? null : TrackConvert.DATE_TIME_FORMATTER.format(dto.getEndTime())));
            if (!snapshot.isEmpty()) {
                sessionManager.send(sub.getSession(), new TrackWsMessage()
                        .setType(TrackWsMessage.TYPE_INIT)
                        .setData(TrackConvert.toVOList(snapshot)));
            }
            log.info("会话 {} 订阅成功: target={}, time=[{}, {}], 快照 {} 条, 会话水位 {}",
                    session.getId(), dto.getTarget(), dto.getStartTime(), dto.getEndTime(), snapshot.size(), sub.getLastPushedId());
        }
    }

    /**
     * 取消订阅（幂等；主动退订与连接关闭两个入口均调用此处。
     * 结构操作由 ConcurrentHashMap 保证安全，无需持锁）
     */
    public void unsubscribe(WebSocketSession session) {
        subscriptionManager.remove(session.getId());
        log.info("会话 {} 订阅已移除", session.getId());
    }

    /**
     * 推送轮次:每秒执行一次（fixedDelay 保证上一轮完全结束后再计时，天然无轮次重叠）
     * 与模拟器共用 Spring 默认单线程调度器，任务间串行执行，无并发竞争
     */
    @Scheduled(fixedDelay = 1000)
    public void pushTick() {
        synchronized (pushLock) {
            //无订阅会话时直接跳过，不产生任何数据库查询
            if (subscriptionManager.isEmpty()) {
                return;
            }
            ensureWatermark();
            //按全局水位拉取本批增量（外部手动 INSERT 的数据同样会被扫到）
            List<TrackDetailDTO> rows = trackService.findIncremental(globalWatermark, BATCH_LIMIT);
            long batchMaxId = rows.stream().mapToLong(TrackDetailDTO::getId).max().orElse(globalWatermark);

            for (TrackSubscriptionManager.Subscription sub : subscriptionManager.all()) {
                //单会话异常不影响其他会话的推送
                try {
                    dispatchBatch(sub, rows, batchMaxId);
                    checkWindowClosed(sub);
                } catch (Exception e) {
                    log.error("会话 {} 推送处理异常: {}", sub.getSession().getId(), e.getMessage(), e);
                }
            }
            //轮次结束推进全局水位（只增不减，异常时本轮白扫下次重扫，不丢数据）
            globalWatermark = batchMaxId;
        }
    }

    /**
     * 向单个会话分发增量批次:过滤（会话水位 + 目标 + 采集时间）→ DATA_ADD → 推进会话水位
     * 会话水位推进到批次最大 id（未匹配的行也算"已考虑"，避免历史批次被反复过滤）
     */
    private void dispatchBatch(TrackSubscriptionManager.Subscription sub, List<TrackDetailDTO> rows, long batchMaxId) {
        if (!rows.isEmpty()) {
            List<TrackDetailDTO> matched = rows.stream()
                    .filter(row -> row.getId() > sub.getLastPushedId())
                    .filter(row -> matchTarget(sub, row))
                    .filter(row -> matchTimeWindow(sub, row))
                    .collect(java.util.stream.Collectors.toList());
            if (!matched.isEmpty()) {
                sessionManager.send(sub.getSession(), new TrackWsMessage()
                        .setType(TrackWsMessage.TYPE_DATA_ADD)
                        .setData(TrackConvert.toVOList(matched)));
            }
            sub.setLastPushedId(Math.max(sub.getLastPushedId(), batchMaxId));
        }
    }

    /**
     * 窗口关闭检测:now > endTime 时一次性推送 WINDOW_CLOSED
     * 注意:通知后不停止增量过滤——迟到补录数据（collect_time 仍在窗口内）继续推 DATA_ADD
     */
    private void checkWindowClosed(TrackSubscriptionManager.Subscription sub) {
        if (!sub.isWindowClosedNotified() && sub.getEndTime() != null
                && java.time.LocalDateTime.now().isAfter(sub.getEndTime())) {
            sub.setWindowClosedNotified(true);
            sessionManager.send(sub.getSession(), new TrackWsMessage()
                    .setType(TrackWsMessage.TYPE_WINDOW_CLOSED)
                    .setEndTime(TrackConvert.DATE_TIME_FORMATTER.format(sub.getEndTime()))
                    .setMessage("订阅时间窗口已结束，不再有新的实时数据；迟到补录数据若仍在窗口内会继续推送"));
            log.info("会话 {} 订阅窗口已关闭: endTime={}", sub.getSession().getId(), sub.getEndTime());
        }
    }

    /**
     * 目标过滤:null 关键字 = 全部目标；否则按目标名称模糊匹配（contains，与 SQL LIKE 语义一致）
     */
    private boolean matchTarget(TrackSubscriptionManager.Subscription sub, TrackDetailDTO row) {
        return sub.getNameFilter() == null
                || (row.getTargetName() != null && row.getTargetName().contains(sub.getNameFilter()));
    }

    /**
     * 时间窗口过滤（闭区间，按数据自身采集时间，与推送/到达时刻无关——迟到数据语义）
     */
    private boolean matchTimeWindow(TrackSubscriptionManager.Subscription sub, TrackDetailDTO row) {
        if (row.getCollectTime() == null) {
            return false;
        }
        boolean afterStart = sub.getStartTime() == null || !row.getCollectTime().isBefore(sub.getStartTime());
        boolean beforeEnd = sub.getEndTime() == null || !row.getCollectTime().isAfter(sub.getEndTime());
        return afterStart && beforeEnd;
    }

    /**
     * 懒初始化全局水位:首个有会话的轮次用表内 max(id) 初始化，历史存量只经 INIT 快照下发
     */
    private void ensureWatermark() {
        if (!watermarkInitialized) {
            globalWatermark = trackService.maxId();
            watermarkInitialized = true;
            log.info("全局推送水位初始化为 {}", globalWatermark);
        }
    }
}

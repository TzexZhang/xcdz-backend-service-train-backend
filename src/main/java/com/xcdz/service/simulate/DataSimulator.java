package com.xcdz.service.simulate;

import cn.hutool.core.util.RandomUtil;
import com.xcdz.service.entity.Target;
import com.xcdz.service.entity.Track;
import com.xcdz.service.service.TargetService;
import com.xcdz.service.service.TrackService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 软件单元:JAVA-SIM-DATA_SIMULATOR-001
 * 功能:数据模拟器——扮演"第三方系统"持续向 track 表写入批次采集数据
 * <p>
 * 设计说明（ADR）:
 * 1. 与推送侧（TrackPushTask）完全解耦:本类只管 INSERT，不做任何推送，
 *    用于验证"水位轮询可感知任意写入方"（业务代码/模拟器/外部手动 SQL 均可被推送）；
 * 2. 用 1 秒 tick + 可配置间隔Millis 实现运行期动态调频（@Scheduled 的 fixedDelay
 *    无法运行期修改，故用 tick 内累计判断的方式模拟可变周期）；
 * 3. 每轮以 20% 概率将 collect_time 回拨 3~10 秒，模拟"迟到补录数据"——
 *    用于演示窗口关闭(WINDOW_CLOSED)后，collect_time 仍在窗口内的数据仍会被推送。
 * 线程安全:running(AtomicBoolean)/intervalMillis(volatile) 由 REST 接口与调度线程并发读写
 */
@Slf4j
@Component
public class DataSimulator {

    // 仅get set ：使用 volatile 即可；先校验再修改  必须使用 Atomicxxx系列
    //模拟开关（默认开启，POST /simulate/start|stop 切换），无锁原子读改写
    private final AtomicBoolean running = new AtomicBoolean(true);

    //写入间隔毫秒（POST /simulate/interval 动态调整），volatile 保证跨线程可见
    private volatile long intervalMillis = 2000L;

    //上次实际写入的时间戳（System.currentTimeMillis，用于 tick 内累计判断是否到达写入周期）
    private volatile long lastWriteMillis = 0L;

    //目标表持续为空的告警只打一次，避免每 2 秒刷屏（目标表来数据后复位）
    private boolean emptyWarned = false;

    @Autowired
    private TargetService targetService;

    @Autowired
    private TrackService trackService;

    /**
     * 1 秒 tick:开关开启且距上次写入达到配置间隔时执行一轮写入
     * fixedDelay 保证轮次不重叠；与推送任务共用单线程调度器，串行执行：上一次执行结束 → 下一次开始的间隔
     */
    @Scheduled(fixedDelay = 5000)
    public void simulateTick() {
        if (!running.get()) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastWriteMillis < intervalMillis) {
            return; //未到写入周期，跳过本 tick
        }
        lastWriteMillis = now;
        try {
            writeBatch();
        } catch (Exception e) {
            //写入失败不影响下一轮（如库短暂不可用），只记录日志
            log.error("模拟数据写入失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 单轮写入:随机选 1~3 个目标，每个目标 1~2 条批次记录，批量 INSERT
     */
    private void writeBatch() {
        List<Target> targets = targetService.listAll();
        if (targets.isEmpty()) {
            if (!emptyWarned) {
                log.warn("target 表为空，模拟器跳过写入——请先执行 sql/schema.sql 初始化目标数据");
                emptyWarned = true;
            }
            return;
        }
        emptyWarned = false;

        LocalDateTime now = LocalDateTime.now();
        List<Track> batch = new ArrayList<>();
        //随机选 1~3 个目标（randomInt 上界 exclusive）
        int nodeCount = RandomUtil.randomInt(1, 4);
        for (int i = 0; i < nodeCount; i++) {
            // 从集合/数组中随机挑一个元素
            String targetId = RandomUtil.randomEle(targets).getId();
            //每个目标 1~2 条
            int rowCount = RandomUtil.randomInt(1, 3);
            for (int j = 0; j < rowCount; j++) {
                batch.add(new Track()
                        .setTargetId(targetId)
                        //采集指标:0~100 随机值，2 位小数（无量纲演示值）
                        .setMetricValue(BigDecimal.valueOf(RandomUtil.randomDouble(0, 100, 2, RoundingMode.HALF_UP)))
                        .setCollectTime(buildCollectTime(now))
                        .setCreateTime(now));
            }
        }
        trackService.saveRecords(batch);
        log.info("模拟器写入 {} 条数据（目标数 {}，间隔 {}ms）", batch.size(), nodeCount, intervalMillis);
    }

    /**
     * 采集时间生成:默认当前时刻；20% 概率回拨 3~10 秒模拟迟到补录数据
     * （迟到数据用于验证:窗口关闭后按"数据自身时间"过滤仍可推送）
     * minusSeconds 返回的是 now 之前的某个时间，即比 now 早
     */
    private LocalDateTime buildCollectTime(LocalDateTime now) {
        if (RandomUtil.randomInt(1, 101) <= 20) {
            return now.minusSeconds(RandomUtil.randomInt(3, 11));
        }
        return now;
    }

    /**
     * 开启模拟（幂等）
     */
    public void start() {
        running.set(true);
    }

    /**
     * 停止模拟（幂等；停止的只是新数据写入，存量数据与推送链路不受影响）
     */
    public void stop() {
        running.set(false);
    }

    /**
     * 动态调整写入间隔
     *
     * @param millis 间隔毫秒，最小 500（防止误配为 0/负数打爆数据库）
     */
    public void setInterval(long millis) {
        this.intervalMillis = Math.max(500L, millis);
    }

    /**
     * 当前模拟状态（供 REST 查询与日志输出）
     */
    public boolean isRunning() {
        return running.get();
    }

    public long getIntervalMillis() {
        return intervalMillis;
    }
}

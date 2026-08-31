package com.xcdz.service.push.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xcdz.service.dto.TrackDetailDTO;
import com.xcdz.service.push.TrackPushTask;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * 软件单元:JAVA-CDC-TRACK_CONSUMER-001
 * 功能:CDC 事件消费入口——消费 Debezium 写入 Kafka 的变更事件，
 * 解析为业务 DTO 后委托推送任务分发（track 表），并维护目标维度缓存（target 表）
 * <p>
 * 事件格式（Debezium JsonConverter，schemas.enable=false）:
 * {"before":{...}, "after":{...}, "op":"c|u|d|r", "ts_ms":处理时刻,
 *  "source":{"ts_ms":binlog事务提交时刻, ...}}
 * <p>
 * 核心机制:
 * 1. 时间语义:Debezium 将 MySQL DATETIME（无时区语义）按 UTC 序列化为 epoch millis，
 *    还原 LocalDateTime 必须按 ZoneOffset.UTC——用系统默认时区会差 8 小时（经典陷阱）；
 * 2. 数值语义:connector 配置 decimal.handling.mode=string，
 *    DECIMAL 列以字符串承载（"12.34"），规避 Base64 二进制编码；
 * 3. 投递语义:Kafka at-least-once，同批重复由推送侧分发前按 id 收敛，
 *    跨轮重复由推送侧会话水位（lastPushedId）消化；
 * 4. 可观测性:Micrometer 埋点——cdc.lag（binlog 事务提交→应用处理的端到端延迟分布，
 *    基准取 source.ts_ms 而非顶层 ts_ms）与 cdc.events（按 op 分类的事件计数），
 *    经 /actuator/metrics 即时可查，接 Prometheus 零改动；
 * 5. track 的 DELETE 事件:协议 REVOKE 预留本期不推送，仅计数与日志
 * 分层契约:本类只做"事件解析与观测"，业务分发规则全部委托 TrackPushTask；
 * 维度一致性委托 TargetNameCache
 */
@Slf4j
@Component
public class TrackCdcConsumer {

    //Debezium 操作类型:创建/更新/删除/快照读
    private static final String OP_CREATE = "c";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";
    private static final String OP_READ = "r";

    @Autowired
    private TrackPushTask pushTask;

    @Autowired
    private TargetNameCache targetCache;

    @Autowired
    private MeterRegistry meterRegistry;

    /**
     * track 表变更事件（批量消费，一批一次持锁分发）
     * topic 名经 push.cdc.track-topic 配置，默认 xcdz.train.track（Debezium 默认命名）
     */
    @KafkaListener(topics = "${push.cdc.track-topic:xcdz.train.track}", groupId = "xcdz-push")
    public void onTrackEvents(List<String> records) {
        List<TrackDetailDTO> events = new ArrayList<>(records.size());
        for (String value : records) {
            //Debezium 会在 delete 事件后发送 value 为 null 的 tombstone，直接跳过
            if (value == null || value.isEmpty()) {
                continue;
            }
            JSONObject envelope = JSON.parseObject(value);
            String op = envelope.getString("op");
            if (OP_DELETE.equals(op)) {
                countEvent(OP_DELETE);
                log.info("track DELETE 事件到达（REVOKE 推送协议预留，本期仅记录）: before={}",
                        envelope.getJSONObject("before"));
                continue;
            }
            JSONObject after = envelope.getJSONObject("after");
            //无 after 载荷的事件（心跳/空事务）跳过
            if (after == null) {
                continue;
            }
            events.add(toDetail(after));
            recordLag(envelope);
            countEvent(normalizeOp(op));
        }
        if (!events.isEmpty()) {
            pushTask.onCdcEvents(events);
        }
    }

    /**
     * target 表变更事件（维度缓存维护；改名/新增/删除联动重算订阅路由分组）
     * topic 名经 push.cdc.target-topic 配置，默认 xcdz.train.target
     */
    @KafkaListener(topics = "${push.cdc.target-topic:xcdz.train.target}", groupId = "xcdz-push")
    public void onTargetEvents(List<String> records) {
        for (String value : records) {
            if (value == null || value.isEmpty()) {
                continue;
            }
            JSONObject envelope = JSON.parseObject(value);
            String op = envelope.getString("op");
            if (OP_DELETE.equals(op)) {
                JSONObject before = envelope.getJSONObject("before");
                if (before != null) {
                    targetCache.onTargetDeleted(before.getString("id"));
                }
                continue;
            }
            JSONObject after = envelope.getJSONObject("after");
            if (after != null) {
                targetCache.onTargetUpsert(after.getString("id"), after.getString("target_name"));
            }
        }
    }

    /**
     * Debezium 行事件 after 载荷 → 业务 DTO（targetName 由维度缓存补齐，等价原 LEFT JOIN）
     */
    private TrackDetailDTO toDetail(JSONObject after) {
        String targetId = after.getString("target_id");
        return new TrackDetailDTO()
                .setId(after.getString("id"))
                .setTargetId(targetId)
                .setTargetName(targetCache.name(targetId))
                .setMetricValue(toDecimal(after.getString("metric_value")))
                .setCollectTime(toLocalDateTimeUtc(after.getLong("collect_time")))
                .setCreateTime(toLocalDateTimeUtc(after.getLong("create_time")));
    }

    /**
     * DECIMAL 列（decimal.handling.mode=string）→ BigDecimal；列为 NULL 时保持 null
     */
    private BigDecimal toDecimal(String value) {
        return value == null ? null : new BigDecimal(value);
    }

    /**
     * DATETIME 列（Debezium 按 UTC 序列化的 epoch millis）→ LocalDateTime；
     * 必须显式按 UTC 还原——DATETIME 本身无时区语义，用系统时区会引入 ±8 小时偏移
     */
    private LocalDateTime toLocalDateTimeUtc(Long epochMillis) {
        return epochMillis == null ? null
                : LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }

    /**
     * 端到端延迟采样:source.ts_ms（binlog 事务提交时刻，延迟的正确起点）
     * 与当前时刻之差，记入 cdc.lag 分布（/actuator/metrics/cdc.lag 可查 count/max/mean）
     */
    private void recordLag(JSONObject envelope) {
        JSONObject source = envelope.getJSONObject("source");
        Long binlogTs = source == null ? null : source.getLong("ts_ms");
        if (binlogTs != null) {
            meterRegistry.summary("cdc.lag").record(System.currentTimeMillis() - binlogTs);
        }
    }

    /**
     * 事件计数:cdc.events{op=create|update|read}（delete 在删除分支单独计数）
     */
    private void countEvent(String op) {
        meterRegistry.counter("cdc.events", "op", op).increment();
    }

    /**
     * op 归一化:r（快照读，本方案 snapshot.mode=schema_only 下不应出现）并入 create 语义防御
     */
    private String normalizeOp(String op) {
        return OP_READ.equals(op) ? OP_CREATE : op;
    }
}

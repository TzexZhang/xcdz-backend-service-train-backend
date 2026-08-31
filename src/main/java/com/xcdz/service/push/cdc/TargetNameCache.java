package com.xcdz.service.push.cdc;

import com.xcdz.service.entity.Target;
import com.xcdz.service.push.TrackSubscriptionManager;
import com.xcdz.service.service.TargetService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 软件单元:JAVA-CDC-TARGET_NAME_CACHE-001
 * 功能:目标维度缓存——target_id → target_name 的进程内映射，
 * 为 track 的 CDC 事件补齐 targetName（事件本身只携带 target_id，联表信息由缓存组合）
 * <p>
 * 设计说明（星型模型的维度表处理，企业标准做法）:
 * 1. 事实表（track）走事件流，维度表（target）走本地缓存——避免每条事件回库联表；
 * 2. 一致性:启动全量加载 + target 表 CDC 事件（c/u/d）增量维护，
 *    目标改名/新增/删除实时生效；
 * 3. 联动职责:target 名称变更会使既有订阅关键字的命中关系失效，
 *    变更后驱动订阅管理器重算倒排路由分组（rebind），
 *    使后续事件按新名称正确路由（历史事件不重放，符合增量语义）
 * 线程安全:缓存与路由重算由 Kafka 消费线程驱动，读取方为推送线程，
 * ConcurrentHashMap + 并发集合保证结构安全
 */
@Slf4j
@Component
public class TargetNameCache {

    @Autowired
    private TargetService targetService;

    @Autowired
    private TrackSubscriptionManager subscriptionManager;

    //维度缓存：targetId → targetName
    private final Map<String, String> cache = new ConcurrentHashMap<>();

    /**
     * 启动全量加载（bean 初始化时执行一次；此后的变更由 target 表 CDC 事件维护）
     */
    @PostConstruct
    public void load() {
        for (Target t : targetService.listAll()) {
            cache.put(t.getId(), t.getTargetName());
        }
        log.info("目标维度缓存初始化完成: {} 个目标", cache.size());
    }

    /**
     * 查询目标名称（track 事件组装 DTO 时调用）
     *
     * @return 目标名称；缓存未命中（维度行缺失）返回 null，
     * 与联表 LEFT JOIN 的 null 语义一致——不丢事实行
     */
    public String name(String targetId) {
        return targetId == null ? null : cache.get(targetId);
    }

    /**
     * 反查名称关键字的命中分组（订阅登记时展开倒排路由用）
     *
     * @param keyword 目标名称模糊关键字（与 SQL LIKE / contains 语义一致）
     * @return 当前名称包含关键字的全部 targetId；关键字为 null 时返回 null（表示 wildcard 全域）
     */
    public Set<String> matchingTargetIds(String keyword) {
        if (keyword == null) {
            return null;
        }
        Set<String> ids = new HashSet<>();
        cache.forEach((id, name) -> {
            if (name != null && name.contains(keyword)) {
                ids.add(id);
            }
        });
        return ids;
    }

    /**
     * 维度新增/变更（target 表 CDC c/u 事件）:更新缓存并重算全部关键字订阅的路由分组
     */
    public void onTargetUpsert(String targetId, String targetName) {
        cache.put(targetId, targetName);
        rebindAll();
        log.info("目标维度变更: id={}, name={}（已重算订阅路由分组）", targetId, targetName);
    }

    /**
     * 维度删除（target 表 CDC d 事件）:移除缓存并重算路由分组
     */
    public void onTargetDeleted(String targetId) {
        cache.remove(targetId);
        rebindAll();
        log.info("目标维度删除: id={}（已重算订阅路由分组）", targetId);
    }

    /**
     * 重算全部关键字订阅的倒排路由分组（wildcard 订阅与名称无关，跳过）；
     * 目标表数据量小且变更频率极低，全量重算的成本可忽略
     */
    private void rebindAll() {
        for (TrackSubscriptionManager.Subscription sub : subscriptionManager.all()) {
            if (sub.getNameFilter() != null) {
                subscriptionManager.rebind(sub, matchingTargetIds(sub.getNameFilter()));
            }
        }
    }
}


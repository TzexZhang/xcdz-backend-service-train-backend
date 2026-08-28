package com.xcdz.service.service;

import com.xcdz.service.dto.TrackDetailDTO;
import com.xcdz.service.entity.Track;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 软件单元:JAVA-SERV-TRACK-001
 * 功能:批次数据服务接口（定义"能做什么"）
 * 分层契约:本层只做业务编排——参数校验/业务规则/事务边界；
 * 所有数据访问（条件查询、水位增量、最大id、插入）均委托 TrackMapper，
 * 本层不出现任何 Wrapper/SQL 概念，也不继承 MP 的 IService（避免内置 CRUD 绕过分层）
 */
public interface TrackService {

    /**
     * 按目标名称模糊关键字 + 采集时间范围条件查询（订阅 INIT 快照与 REST 对账共用），
     * 联表组合目标名称
     *
     * @param target    目标名称模糊关键字；null 或空白表示不限制（全部目标）
     * @param startTime 采集时间下界（含）；null 表示不限制
     * @param endTime   采集时间上界（含）；null 表示不限制
     * @param limit     返回条数上限
     * @return 命中记录（含目标名称），按 id 升序
     */
    List<TrackDetailDTO> findByCondition(String target, LocalDateTime startTime, LocalDateTime endTime, int limit);

    /**
     * 水位增量查询:查出所有 id 大于指定水位的记录（推送侧感知"第三方新入库数据"的核心），
     * 联表组合目标名称
     *
     * @param afterId 水位值（只返回 id > afterId 的行）
     * @param limit   单批上限
     * @return 按 id 升序的新记录（含目标名称）；无新数据时返回空集合
     */
    List<TrackDetailDTO> findIncremental(long afterId, int limit);

    /**
     * 查询当前表内最大 id（推送服务首次运行时初始化全局水位用）
     *
     * @return 最大 id；表为空时返回 0
     */
    long maxId();

    /**
     * 批量写入批次数据（模拟器扮演"第三方"的入口；整体一个事务，失败全回滚）
     *
     * @param records 待写入数据集合；空集合直接返回
     */
    void saveRecords(List<Track> records);
}

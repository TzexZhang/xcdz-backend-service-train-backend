package com.xcdz.service.service;

import com.xcdz.service.dto.TrackDetailDTO;
import com.xcdz.service.entity.Track;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 软件单元:JAVA-SERV-TRACK-001
 * 功能:批次数据服务接口（定义"能做什么"）
 * 分层契约:本层只做业务编排——参数校验/业务规则/事务边界；
 * 所有数据访问（条件游标查询、插入）均委托 TrackMapper，
 * 本层不出现任何 Wrapper/SQL 概念，也不继承 MP 的 IService（避免内置 CRUD 绕过分层）
 *
 * 感知方式说明（CDC 方案）:新数据感知已整体切换为 binlog CDC（Debezium→Kafka），
 * 本层不再提供水位增量查询/最大id查询；条件查询仅服务于订阅快照的游标分页
 */
public interface TrackService {

    /**
     * 按目标名称模糊关键字 + 采集时间范围条件查询（订阅流式完整快照的游标分页专用），
     * 联表组合目标名称
     *
     * @param targetName 目标名称模糊关键字；null 或空白表示不限制（全部目标）
     * @param startTime 采集时间下界（含）；null 表示不限制
     * @param endTime   采集时间上界（含）；null 表示不限制
     * @param afterId   id 游标（只返回 id > afterId 的行；null/空白 = 首页不限制；
     *                  id 为 19 位定长数字雪花字符串，字符串比较等价数值序）
     * @param limit     返回条数上限（页大小）
     * @return 命中记录（含目标名称），按 id 升序；查不满一页即表示已到末页
     */
    List<TrackDetailDTO> findByCondition(String targetName, LocalDateTime startTime, LocalDateTime endTime,
                                         String afterId, int limit);

    /**
     * 批量写入批次数据（模拟器扮演"第三方"的入口；整体一个事务，失败全回滚）
     *
     * @param records 待写入数据集合；空集合直接返回
     */
    void saveRecords(List<Track> records);
}

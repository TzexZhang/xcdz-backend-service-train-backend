package com.xcdz.service.dto;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 软件单元:JAVA-DTO-TRACK_DETAIL-001
 * 功能:批次数据联表查询结果（track LEFT JOIN target 组合目标名称，
 * Mapper → Service → 推送/REST 出参转换的进程内数据载体）
 * 设计说明:
 * 1. 字段 = track 表全列 + target.target_name（联表组合列），
 *    列名与属性的驼峰映射由 map-underscore-to-camel-case 完成，XML 不写 resultMap；
 * 2. LEFT JOIN 语义:target 行缺失时 targetName 为 null 而 track 行不丢失——
 *    查询行数与推送水位/对账逻辑不受目标表数据一致性影响；
 * 3. 纯数据载体，不做任何转换处理；出参组装由 utils/TrackConvert 承担
 */
@Data
@Accessors(chain = true)
public class TrackDetailDTO implements Serializable {
    //track.id 自增主键（单调递增，推送水位依据）
    private Long id;

    //所属目标id → target.id（前端"按目标筛选"的关联字段）
    private String targetId;

    //目标名称（LEFT JOIN target.target_name；目标行缺失时为 null）
    private String targetName;

    //采集指标值（无量纲演示值，保留 2 位小数）
    private BigDecimal metricValue;

    //数据自身采集时间（前端时间范围筛选依据；可能晚于入库时间 = 迟到补录数据）
    private LocalDateTime collectTime;

    //入库时间（用于区分"数据发生时刻"与"写入数据库时刻"）
    private LocalDateTime createTime;
}

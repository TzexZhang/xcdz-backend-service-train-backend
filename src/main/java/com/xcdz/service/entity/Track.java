package com.xcdz.service.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 软件单元:JAVA-ENTITY-TRACK-001
 * 功能:批次数据实体（每行一条采集数据，由模拟器扮演的"第三方"持续写入）
 */
@Data
@Accessors(chain = true)
@TableName("track")
public class Track implements Serializable {
    //数据id（String 雪花id，MP assign_id 生成，与项目惯例统一；
    //19 位定长数字字符串，趋势单调递增且字典序与数值序一致，
    //推送侧水位游标（id > afterId）与按 id 升序翻页的语义不受类型变更影响）
    @TableId(type = IdType.ASSIGN_ID)
    private String id;

    //所属目标id → target.id（前端"按目标筛选"的关联字段）
    private String targetId;

    //采集指标值（无量纲演示值，保留 2 位小数）
    private BigDecimal metricValue;

    //数据自身采集时间（前端时间范围筛选依据；可能晚于入库时间 = 迟到补录数据）
    private LocalDateTime collectTime;

    //入库时间（用于区分"数据发生时刻"与"写入数据库时刻"）
    private LocalDateTime createTime;
}

package com.xcdz.service.vo;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 软件单元:JAVA-VO-TRACK-001
 * 功能:批次数据出参对象（后端→前端，REST 对账接口与 WebSocket 推送共用）
 * 设计说明:
 * 1. 本类为纯数据载体，只定义字段，不做任何转换/格式化处理——
 *    实体→VO 的转换由 utils/TrackConvert 承担；
 * 2. 时间字段为 String（服务端已格式化为 yyyy-MM-dd HH:mm:ss），因为该 VO 同时经过
 *    Jackson（REST）与 fastjson（WebSocket）两种序列化器，统一字符串保证输出一致
 */
@Data
@Accessors(chain = true)
public class TrackVO implements Serializable {
    //数据id（自增，单调递增）
    private Long id;

    //所属目标id
    private String targetId;

    //目标名称（联表 target.target_name 组合；目标行缺失时为 null）
    private String targetName;

    //采集指标值（无量纲演示值，保留 2 位小数）
    private BigDecimal metricValue;

    //采集时间（数据自身时间，格式 yyyy-MM-dd HH:mm:ss）
    private String collectTime;

    //入库时间（格式 yyyy-MM-dd HH:mm:ss）
    private String createTime;
}

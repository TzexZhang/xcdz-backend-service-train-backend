package com.xcdz.service.utils;

import com.xcdz.service.dto.TrackDetailDTO;
import com.xcdz.service.vo.TrackVO;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 软件单元:JAVA-CONVERT-TRACK-001
 * 功能:批次数据 联表结果→VO 转换器（VO 保持纯数据载体，转换职责集中于此）
 * 入参说明:查询结果自 Mapper 联表化后为 TrackDetailDTO（含目标名称），
 * 写入路径仍用 Track 实体，不经本转换器
 * DATE_TIME_FORMATTER 同时是 WebSocket 订阅报文与 REST 查询参数的时间格式契约，
 * 供各入口层（Controller/Processor）解析入参时复用
 */
public class TrackConvert {

    /** 统一时间格式契约:yyyy-MM-dd HH:mm:ss（入参解析与出参格式化共用） */
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private TrackConvert() {
        //工具类禁止实例化
    }

    /**
     * 联表结果 → VO（时间字段统一格式化为字符串）
     *
     * @param detail 批次数据联表查询结果（含目标名称）
     * @return 出参对象；detail 为 null 时返回 null
     */
    public static TrackVO toVO(TrackDetailDTO detail) {
        if (detail == null) {
            return null;
        }
        return new TrackVO()
                .setId(detail.getId())
                .setTargetId(detail.getTargetId())
                .setTargetName(detail.getTargetName())
                .setMetricValue(detail.getMetricValue())
                .setCollectTime(detail.getCollectTime() == null ? null : DATE_TIME_FORMATTER.format(detail.getCollectTime()))
                .setCreateTime(detail.getCreateTime() == null ? null : DATE_TIME_FORMATTER.format(detail.getCreateTime()));
    }

    /**
     * 联表结果列表 → VO 列表
     *
     * @param details 联表查询结果集合；null 视为空集合
     * @return VO 列表（不包含 null 元素）
     */
    public static List<TrackVO> toVOList(List<TrackDetailDTO> details) {
        if (details == null || details.isEmpty()) {
            return List.of();
        }
        return details.stream()
                .map(TrackConvert::toVO)
                .collect(Collectors.toList());
    }
}

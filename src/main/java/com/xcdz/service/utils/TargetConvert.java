package com.xcdz.service.utils;

import com.xcdz.service.entity.Target;
import com.xcdz.service.vo.TargetVO;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 软件单元:JAVA-CONVERT-TARGET-001
 * 功能:目标 实体→VO 转换器（VO 保持纯数据载体，转换职责集中于此）
 * 说明:TargetVO 时间字段为 LocalDateTime（仅 REST/Jackson 通道使用，无跨序列化器一致性问题）
 */
public class TargetConvert {

    private TargetConvert() {
        //工具类禁止实例化
    }

    /**
     * 实体 → VO
     *
     * @param entity 目标实体
     * @return 出参对象；entity 为 null 时返回 null
     */
    public static TargetVO toVO(Target entity) {
        if (entity == null) {
            return null;
        }
        return new TargetVO()
                .setId(entity.getId())
                .setTargetName(entity.getTargetName())
                .setCreateTime(entity.getCreateTime());
    }

    /**
     * 实体列表 → VO 列表
     *
     * @param entities 实体集合；null 视为空集合
     * @return VO 列表（不包含 null 元素）
     */
    public static List<TargetVO> toVOList(List<Target> entities) {
        if (entities == null || entities.isEmpty()) {
            return List.of();
        }
        return entities.stream()
                .map(TargetConvert::toVO)
                .collect(Collectors.toList());
    }
}

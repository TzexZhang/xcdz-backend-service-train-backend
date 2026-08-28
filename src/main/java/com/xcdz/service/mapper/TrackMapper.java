package com.xcdz.service.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.xcdz.service.dto.TrackDetailDTO;
import com.xcdz.service.entity.Track;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 软件单元:JAVA-MAPPER-TRACK-001
 * 功能:批次数据访问层（操作 track 表，被第三方持续写入的数据表）
 * 职责边界:本层封装全部数据访问（条件查询/水位增量/最大id/单条插入），
 * Service 层只做业务编排，不直接构造任何查询条件
 * SQL 归属:条件查询与水位增量需 LEFT JOIN target 组合目标名称，
 * Wrapper 不支持多表，两方法为抽象方法，SQL 在 resources/mapper/TrackMapper.xml；
 * 其余单表能力（最大id/插入）仍用 BaseMapper + Wrapper
 */
@Mapper
public interface TrackMapper extends BaseMapper<Track> {

    /**
     * 按目标名称模糊关键字 + 采集时间范围条件查询（订阅 INIT 快照与 REST 对账共用），
     * LEFT JOIN target 组合目标名称并按 target_name LIKE 过滤
     * （SQL 见 TrackMapper.xml 的 selectByCondition）
     *
     * @param target    目标名称模糊关键字；null 或空白表示不限制（全部目标）
     * @param startTime 采集时间下界（含）；null 表示不限制
     * @param endTime   采集时间上界（含）；null 表示不限制
     * @param limit     返回条数上限（由 Service 层校验为正整数后传入，预编译占位传入）
     * @return 命中记录（含目标名称），按 id 升序
     */
    List<TrackDetailDTO> selectByCondition(@Param("target") String target,
                                           @Param("startTime") LocalDateTime startTime,
                                           @Param("endTime") LocalDateTime endTime,
                                           @Param("limit") int limit);

    /**
     * 水位增量查询:查出所有 id 大于指定水位的记录（推送侧感知"第三方新入库数据"的核心），
     * LEFT JOIN target 组合目标名称（SQL 见 TrackMapper.xml 的 selectIncremental）；
     * 依赖 track.id 自增主键的单调性；同一行不会被重复扫描（水位只增不减）
     *
     * @param afterId 水位值（只返回 id > afterId 的行）
     * @param limit   单批上限
     * @return 按 id 升序的新记录（含目标名称）；无新数据时返回空集合
     */
    List<TrackDetailDTO> selectIncremental(@Param("afterId") long afterId, @Param("limit") int limit);

    /**
     * 查询当前表内最大 id（推送服务首次运行时初始化全局水位用）
     *
     * @return 最大 id；表为空时返回 0
     */
    default long selectMaxId() {
        LambdaQueryWrapper<Track> wrapper = new LambdaQueryWrapper<Track>()
                .orderByDesc(Track::getId)
                .last("LIMIT 1");
        Track latest = selectOne(wrapper);
        return latest == null ? 0L : latest.getId();
    }
}

package com.xcdz.service.mapper;

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
 * 职责边界:本层封装全部数据访问（条件游标查询/单条插入），
 * Service 层只做业务编排，不直接构造任何查询条件
 * SQL 归属:条件查询需 LEFT JOIN target 组合目标名称，
 * Wrapper 不支持多表，该方法为抽象方法，SQL 在 resources/mapper/TrackMapper.xml；
 * 其余单表能力（插入）用 BaseMapper；
 * 感知方式说明（CDC 方案）:水位增量查询/最大id查询已随轮询感知层退役删除
 */
@Mapper
public interface TrackMapper extends BaseMapper<Track> {

    /**
     * 按目标名称模糊关键字 + 采集时间范围 + id 游标条件查询（订阅流式完整快照的游标分页），
     * LEFT JOIN target 组合目标名称并按 target_name LIKE 过滤
     * （SQL 见 TrackMapper.xml 的 selectByCondition）
     *
     * @param targetName 目标名称模糊关键字；null 或空白表示不限制（全部目标）
     * @param startTime 采集时间下界（含）；null 表示不限制
     * @param endTime   采集时间上界（含）；null 表示不限制
     * @param afterId   id 游标（只返回 id > afterId 的行；null/空白 = 首页不限制；
     *                  id 为 19 位定长数字雪花字符串，字符串比较等价数值序）
     * @param limit     返回条数上限（由 Service 层校验为正整数后传入，预编译占位传入）
     * @return 命中记录（含目标名称），按 id 升序；查不满一页即表示已到末页
     */
    List<TrackDetailDTO> selectByCondition(@Param("targetName") String targetName,
                                           @Param("startTime") LocalDateTime startTime,
                                           @Param("endTime") LocalDateTime endTime,
                                           @Param("afterId") String afterId,
                                           @Param("limit") int limit);
}

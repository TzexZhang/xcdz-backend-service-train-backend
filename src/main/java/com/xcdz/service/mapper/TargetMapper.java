package com.xcdz.service.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.xcdz.service.entity.Target;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * 软件单元:JAVA-MAPPER-TARGET-001
 * 功能:目标数据访问层（操作 target 表）
 * 职责边界:本层封装全部数据访问，Service 层只做业务编排
 */
@Mapper
public interface TargetMapper extends BaseMapper<Target> {

    /**
     * 查询全部目标（按创建时间升序，保证前端树节点顺序稳定）
     *
     * @return 全部目标；表为空时返回空集合
     */
    default List<Target> selectAll() {
        LambdaQueryWrapper<Target> wrapper = new LambdaQueryWrapper<Target>()
                .orderByAsc(Target::getCreateTime);
        return selectList(wrapper);
    }
}

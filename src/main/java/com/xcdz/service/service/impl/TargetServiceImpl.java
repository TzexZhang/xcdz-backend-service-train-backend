package com.xcdz.service.service.impl;

import com.xcdz.service.entity.Target;
import com.xcdz.service.mapper.TargetMapper;
import com.xcdz.service.service.TargetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 软件单元:JAVA-SERV-TARGET-001
 * 功能:目标服务实现（业务编排层，数据访问委托 Mapper，不继承 MP 的 ServiceImpl）
 */
@Service
public class TargetServiceImpl implements TargetService {

    @Autowired
    private TargetMapper targetMapper;

    /**
     * 查询全部目标:当前无额外业务规则，直接委托 Mapper
     */
    @Override
    public List<Target> listAll() {
        return targetMapper.selectAll();
    }
}
